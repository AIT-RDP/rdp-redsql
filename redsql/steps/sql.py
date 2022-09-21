"""
Implements the steps that involve secondary database interactions (e.g. to resolve some values
"""
from typing import Dict, Any, Optional, Iterable

import sqlalchemy as sql

import redsql.steps.abc.step as abstract_step
import redsql.exc as exc


class CachedSQLQuery(abstract_step.AbstractOneToOneStep):
    """
    Implements a transformation step that executes an SQL statements and appends the result to the output message.

    To improve the performance, results are heavily cached. Hence, it is advised to only use the step for static
    meta-data.
    """

    def __init__(self, config: dict, channel_name: str, step_name: str, **kwargs):
        """
        :param config: The user configuration describing the query step.
        :param channel_name: The name of the channel that contains the step
        :param step_name: A unique name of the processing step to simplify debugging
        :param kwargs: The dynamically added rest of any injected parameters
        """
        super(CachedSQLQuery, self).__init__(**kwargs)

        self._cache_keys = config["cache keys"]
        if not isinstance(self._cache_keys, list):
            raise ValueError(f"A list of cache keys for {channel_name}.{step_name} is expected but an object of "
                             f" type {type(self._cache_keys)} is found.")

        self._sql_statement = sql.text(config["query"])
        self._single_value = bool(config.get("single value", True))
        self._cache = {}
        self._sql_engine: Optional[sql.engine.Engine] = None

    def open(self, sql_engine: sql.engine.Engine, **kwargs):
        """Opens the database connection using the given engine"""

        assert self._sql_engine is None, "open(...) was called before"
        self._sql_engine = sql_engine

    def transform_single_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Executes the query, if necessary, and returns the result"""

        missing_cache_keys = set(self._cache_keys).difference(message.keys())
        if len(missing_cache_keys) > 0:
            raise exc.MessageFormatError(f"The message does not contain the cache keys {list(missing_cache_keys)}",
                                         triggering_message=message)

        cache_key = tuple(message[key] for key in self._cache_keys)
        if cache_key not in self._cache:
            self._cache[cache_key] = self._fetch_query_results(message)

        message = message.copy()
        message.update(**self._cache[cache_key])
        return message

    def _fetch_query_results(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Executes the query and fetches the results in a common message format"""

        assert self._sql_engine is not None
        with self._sql_engine.connect() as sql_connection:
            with sql_connection.begin():
                # Wrap each single operation into a transaction to avoid deadlocks by holding DB resources
                sql_results = sql_connection.execute(self._sql_statement, **message)
        result_data = sql_results.fetchall()

        if self._single_value and len(result_data) != 1:
            raise ValueError(f"The SQL query does not return a single result row but {len(result_data)}")

        if self._single_value:
            return dict(result_data[0])
        else:
            ret = {}
            for row in result_data:
                assert len(ret) == 0 or len(ret) == len(row), "Malformed query result"
                ret = {key: ret.get(key, []) + [new_val] for key, new_val in row.items()}
            return ret

    def close(self):
        """Frees allocated network resources"""
        assert self._sql_engine is not None, "open(...) was not successfully called before"
        self._sql_engine = None
