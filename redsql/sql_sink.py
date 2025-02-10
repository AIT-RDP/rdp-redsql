"""
Implements the SQL table sink that pushes the data to the database
"""
import logging
from typing import Dict, Iterable, Any

import prometheus_client as prom
import sqlalchemy as sql
import sqlalchemy.exc

from redsql import exc as exc


class SQLTableSink:
    """
    Helper class that allows to store messages in database tables

    The class is specifically optimized to exploit the SQLAlchemy insertion mechanisms instead of less performant but
    more generic SQL queries.
    """

    _prom_insert_cnt = prom.Counter("redsql_inserted_table_rows", labelnames=["channel_name"],
                                    documentation="Number of inserted or updated table rows")

    def __init__(self, config: dict, sql_engine: sql.engine.Engine, channel_name: str):
        """

        :param config: The SQL-specific configuration stanza
        :param sql_engine: The DB engine to draw the connections from.
        :param channel_name: The name of the corresponding channel for debugging purpose
        """

        self._channel_name = channel_name
        self._logger = logging.getLogger(f"{__name__}.{channel_name}")

        self._logger.debug(f"Try to access meta data from the SQL engine {sql_engine}")
        self._sql_engine = sql_engine
        self._sql_meta = sql.MetaData(bind=sql_engine)
        self._sql_meta.reflect()
        self._logger.debug(f"SQL schema successfully retrieved.")

        self._destination_table = self._sql_meta.tables[config["table"]]
        self._column_mapping = self._get_column_mapping(config.get("columns", {}), self._destination_table,
                                                        channel_name)
        self._logger.debug(f"Determine the column mapping of {self._destination_table.name}: {self._column_mapping}")

        self._insert_statement = self._compile_insert_statement(
            self._destination_table, self._logger,
            update_duplicates=config.get("update duplicate values", False)
        )

        self._prom_insert_cnt.labels(channel_name=channel_name)

    @staticmethod
    def _get_column_mapping(column_config: dict, destination_table: sql.Table, channel_name: str) -> Dict[str, str]:
        """
        Returns the autocompleted mapping from table columns to message keys

        :param column_config: The partial column definition from the configuration mapping column names to message
            keys as well.
        :param destination_table: The SQL table definition
        :param channel_name: The channel name for debugging purpose
        """

        column_names = [col.name for col in destination_table.columns]

        invalid_config_columns = set(column_config.keys()).difference(column_names)
        if len(invalid_config_columns) > 0:
            raise KeyError(f"The configuration of channel {channel_name}, table {destination_table.name} contains "
                           f"invalid column names: {invalid_config_columns}. Available columns: {column_names}")

        mapping = {col_name: column_config.get(col_name, col_name) for col_name in column_names}
        return mapping

    @staticmethod
    def _compile_insert_statement(destination_table: sql.Table, logger: logging.Logger,
                                  update_duplicates: bool = False):
        """Compiles the SqlAlchemy insert statement according to the given configuration and returns it"""

        if update_duplicates:
            logger.debug(f"Update duplicate values in {destination_table.name}. This feature requires PostgreSQL.")
            import sqlalchemy.dialects.postgresql as pg_dialect  # Requires PostgreSQL

            primary_keys = [c for c in destination_table.constraints if isinstance(c, sql.PrimaryKeyConstraint)]
            if len(primary_keys) != 1:
                raise ValueError(f"It is requested to update duplicate values but {destination_table.name} "
                                 f"does not have a unique primary key: {primary_keys}")

            ins_stmt = pg_dialect.insert(destination_table)

            update_mapping = {
                # PG creates an intermediate excluded table for all invalid statements that needs to be referenced
                col: getattr(ins_stmt.excluded, col.name)
                for col in destination_table.columns if col.name not in primary_keys[0].columns
            }
            ins_stmt = ins_stmt.on_conflict_do_update(index_elements=list(primary_keys[0].columns), set_=update_mapping)
        else:
            ins_stmt = sql.insert(destination_table)

        logger.debug(f"Compiled insert statement: {ins_stmt}")
        return ins_stmt

    def insert_messages(self, messages: Iterable[Dict[str, Any]]):
        """
        Inserts the given messages into the database

        Before inserting, the keys will be mapped according to the columns section in the configuration. In case some
        columns are not defined in the configuration section, it is assumed that each message contains a key with the
        respective column name.

        :param messages: An iterable of messages. Each message must be composed of generic key-value pairs.
        """

        self._logger.debug(f"Start to compute table representation for {self._destination_table.name}")
        output_data = list(map(self._remap_message, messages))

        if len(output_data) <= 0:
            self._logger.debug(f"No message data - skip inserting rows into {self._destination_table.name}")
            return

        self._logger.debug(f"Begin to insert {len(output_data)} row(s) into {self._destination_table.name}")
        try:
            with self._sql_engine.connect() as sql_connection:
                with sql_connection.begin():  # Open a new transaction to avoid caching issues
                    for idx, item in enumerate(output_data):
                        try:
                            # handle null values
                            if 'value' in output_data[idx].keys():
                                if output_data[idx].get('value') is not None:
                                    output_data[idx]['value'] = float(output_data[idx]['value'])
                        except ValueError as e:
                            self._logger.error(f"Impossible to convert '{output_data[idx]['value']}' to float. "
                                               f"The database supports only float values. "
                                               f"Make sure to format the data in the Redis stream accordingly. "
                                               f"The datapoint will not be inserted into the database")
                    sql_connection.execute(self._insert_statement, output_data)

            self._prom_insert_cnt.labels(channel_name=self._channel_name).inc(len(output_data))

        except sqlalchemy.exc.DataError as e:
            new_err = exc.MessageFormatError(f"Unable to insert samples into {self._destination_table.name} using "
                                             f"'{e.statement}' and params {e.params}: {e.detail}, {e.orig}.",
                                             triggering_message=e.params)
            raise new_err from e
        except sqlalchemy.exc.IntegrityError as e:
            new_err = exc.MessageFormatError(f"Integrity error when inserting samples into "
                                             f"{self._destination_table.name} using '{e.statement}' and params "
                                             f"{e.params}: {e.detail}, {e.orig}",
                                             triggering_message=e.params)
            raise new_err from e

        self._logger.debug(f"Successfully inserted {len(output_data)} row(s) into {self._destination_table.name}")

    def _remap_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extracts the column values from the message and returns them"""

        sample_data = {
            col_name: message[message_name]
            for col_name, message_name in self._column_mapping.items() if message_name in message
        }
        return sample_data

    def close(self):
        """Closes the database connection and frees allocated resources"""
        self._sql_engine = None
