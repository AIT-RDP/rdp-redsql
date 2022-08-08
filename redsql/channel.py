"""
Defines facilities to manage channels transforming Redis input messages to SQL statements
"""
import importlib
import inspect
import logging
import threading
from typing import Optional, Dict, Any, List, Iterable

import redis
import sqlalchemy as sql
import sqlalchemy.exc

import redsql.steps.abc.step as abstract_step
import redsql.steps.decoding as decoding_step
import redsql.exc as exc


class _RedisStreamSource:
    """Helper class that fetches messages from the Redis database"""

    def __init__(self, config: dict, redis_pool: redis.ConnectionPool, channel_name: str):
        """
        Initializes the stream source
        :param config: The source-specific trigger stanza
        :param redis_pool: The connection pool to set in the Redis client
        :param channel_name: The name of the related channel for debugging and naming purpose
        """

        self._logger = logging.getLogger(f"{__name__}.{channel_name}")

        self._logger.debug(f"Try to access the Redis instance via {redis_pool}")
        self._redis_client = redis.Redis(connection_pool=redis_pool)
        self._redis_client.ping()
        self._logger.debug(f"Successfully connected to Redis")

        self._stream_name = config["stream id"]
        self._group_name = config.get("group id", f"group.{self._stream_name}.{channel_name}")
        self._consumer_name = config.get("consumer id", f"consumer.{self._stream_name}.{channel_name}")

        self._last_message_id = None

        self._init_redis_streams()

    def _init_redis_streams(self):
        """Initialises a Redis stream if it has not already been initialized"""

        if not self._redis_client.exists(self._stream_name):
            self._logger.info(f"Create Redis group {self._group_name} and stream {self._stream_name}")
            self._redis_client.xgroup_create(name=self._stream_name, groupname=self._group_name, mkstream=True,
                                             id="0-0")  # Also consume messages before the group was created
        else:
            self._logger.info(f"Try creating Redis group {self._group_name} on existing stream {self._stream_name}")
            try:
                # We cannot easily determine whether a group is already existing. Hence, try to create it and re-raise
                # the error in case it is not the expected one. (Thx to Denis and CLUE Data Sync.)
                self._redis_client.xgroup_create(name=self._stream_name, groupname=self._group_name, id="0-0")
            except redis.exceptions.ResponseError as e:
                if not "BUSYGROUP" in str(e):
                    raise

    def get_next_message(self) -> Optional[Dict[str, Any]]:
        """
        Reads the next message or returns None, in case a timeout occurs
        :return: The raw message
        """

        message = self._redis_client.xreadgroup(self._group_name, self._consumer_name, {self._stream_name: ">"},
                                                count=1, block=2000)
        assert len(message) <= 1, "At most one stream result expected"
        if len(message) >= 1 and len(message[0][1]) >= 1:
            assert len(message[0][1]) <= 1, "At most one message expected"
            assert message[0][0] == self._stream_name, "Received message from invalid stream"

            self._last_message_id = message[0][1][0][0]
            message_content = message[0][1][0][1]
            self._logger.debug(f"Received message {self._last_message_id} with keys {list(message_content.keys())}")
        else:
            self._last_message_id = None
            message_content = None

        return message_content

    def ack_last_message(self):
        """Acknowledges the last message before retrieving the next one"""
        assert self._last_message_id is not None, "No message is left to acknowledge"
        self._redis_client.xack(self._stream_name, self._group_name, self._last_message_id)
        self._last_message_id = None


class _SQLTableSink:
    """
    Helper class that allows to store messages in database tables

    The class is specifically optimized to exploit the SQLAlchemy insertion mechanisms instead of less performant but
    more generic SQL queries.
    """

    def __init__(self, config: dict, sql_engine: sql.engine.Engine, channel_name: str):
        """

        :param config: The SQL-specific configuration stanza
        :param sql_engine: The DB engine to draw the connections from.
        :param channel_name: The name of the corresponding channel for debugging purpose
        """

        self._logger = logging.getLogger(f"{__name__}.{channel_name}")

        self._logger.debug(f"Try to access meta data from the SQL engine {sql_engine}")
        self._sql_connection = sql_engine.connect()
        self._sql_meta = sql.MetaData()
        self._sql_meta.reflect(bind=self._sql_connection)
        self._logger.debug(f"SQL schema successfully retrieved.")

        self._destination_table = self._sql_meta.tables[config["table"]]
        self._column_mapping = self._get_column_mapping(config["columns"], self._destination_table, channel_name)
        self._logger.debug(f"Determine the column mapping of {self._destination_table.name}: {self._column_mapping}")

        self._insert_statement = self._compile_insert_statement(
            self._destination_table, self._logger,
            update_duplicates=config.get("update duplicate values", False)
        )

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
            ins_stmt = ins_stmt.on_conflict_do_update(constraint=primary_keys[0].name, set_=update_mapping)
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

        self._logger.debug(f"Start to compute tabel representation for {self._destination_table.name}")
        output_data = list(map(self._remap_message, messages))

        self._logger.debug(f"Begin to insert {len(output_data)} row(s) into {self._destination_table.name}")
        try:
            with self._sql_connection.begin():  # Open a new transaction to avoid caching issues
                self._sql_connection.execute(self._insert_statement, output_data)

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
        self._sql_connection.close()
        self._sql_connection = None


class Channel:
    """A data pipeline with a unified set of processing steps"""

    def __init__(self, channel_config: dict, channel_name: str = "<channel>"):
        """
        Initializes the channel but does not start any processing steps

        :param channel_config: The channel-specific configuration stanza
        :param channel_name: The name of the channel for debugging purpose
        """

        self._logger = logging.getLogger(f"{__name__}.{channel_name}")
        self._config = channel_config
        self._channel_name = channel_name

        self._data_source: Optional[_RedisStreamSource] = None
        self._data_sink: Optional[_SQLTableSink] = None

        self._transformation_steps: List[abstract_step.AbstractTransformationStep] = [
            decoding_step.DecodingStep(config=channel_config.get("encoding", {}), channel_name=channel_name,
                                       step_name="0-decoding")
        ]

        step_config = channel_config.get("steps", [])
        self._transformation_steps += self._instantiate_steps(step_config, channel_name, self._logger)

    @staticmethod
    def _instantiate_steps(step_config: list, channel_name: str,
                           logger: logging.Logger) -> List[abstract_step.AbstractTransformationStep]:
        """
        Dynamically instantiates the processing steps according to the configuration

        :param step_config: The step-specific configuration stanza
        :param channel_name: The channel_name for debugging purpose
        :param logger: Some logger to output debugging information
        """

        step_config = {f"step-{i + 1}": cfg for i, cfg in enumerate(step_config)}  # Create a name for every step
        steps = [
            Channel._instantiate_step(cfg, channel_name, step_name, logger)
            for step_name, cfg in step_config.items()
        ]
        logger.debug(f"Instantiated all {len(steps)} transformation steps of channel '{channel_name}'")
        return steps

    @staticmethod
    def _instantiate_step(step_config: dict, channel_name: str, step_name: str,
                          logger: logging.Logger) -> abstract_step.AbstractTransformationStep:
        """
        Dynamically instantiates the configured transformation step and returns it

        :param step_config: The step-specific configuration stanza
        :param channel_name: The channel_name for debugging purpose
        :param step_name: The name of the step for debugging purpose
        :param logger: Some logger to output debugging information
        """

        type_name = step_config["type"]
        name_components = str(type_name).split(".")
        class_name = name_components[-1]
        if len(name_components) <= 1:
            module_name = "redsql.steps"
        else:
            module_name = ".".join(name_components[:-1])

        logger.debug(f"Instantiate step {step_name} as class {class_name} in {module_name}.")
        step_module = importlib.import_module(module_name)
        assert step_module is not None

        step_class: type = getattr(step_module, name_components[-1])
        if not inspect.isclass(step_class):
            raise ModuleNotFoundError(f"The specified step class '{type_name}' ({step_class}) is not a class.")
        if not issubclass(step_class, abstract_step.AbstractTransformationStep):
            raise ModuleNotFoundError(f"The specified step class '{type_name}' ({step_class}) is not an "
                                      f"AbstractTransformationStep.")

        step_object = step_class(config=step_config, channel_name=channel_name, step_name=step_name)
        return step_object

    def open(self, redis_pool: redis.ConnectionPool, sql_engine: sql.engine.Engine):
        """
        Opens the channel by initializing the Redis and DB client.

        The function has to be executed in the target thread that executes execute_channel_once()

        :param redis_pool: The possibly shared redis connection pool
        :param sql_engine: The possibly shared DB engine to create the connection from
        """

        self._data_source = _RedisStreamSource(self._config["trigger"], redis_pool, self._channel_name)
        self._data_sink = _SQLTableSink(self._config["data sink"], sql_engine, self._channel_name)

        self._logger.debug(f"Initialize external resources on all {len(self._transformation_steps)} steps.")
        for step in self._transformation_steps:
            step.open(sql_engine=sql_engine, redis_pool=redis_pool)

    def execute_channel_once(self):
        """
        Executes the channel once and returns.

        In case no message is received, the function will run into a timeout and return without executing the pipeline.
        """

        assert self._data_source is not None, "Need to open the channel before"

        message = self._data_source.get_next_message()
        if message is not None:

            output_messages = [message]
            try:
                # Run the processing steps and push the result
                for step in self._transformation_steps:
                    output_messages = step.transform_messages(output_messages)
                self._data_sink.insert_messages(output_messages)

            except exc.MessageFormatError as e:
                self._data_source.ack_last_message()  # Permanent error. Remove the message from the queue
                e.external_message = message
                raise

            self._data_source.ack_last_message()

    def close(self):
        """
        Closes the Redis and DB connection.
        """

        assert self._data_sink is not None, "No data sink found. Call open(...) beforehand."

        for step in self._transformation_steps:
            step.close()

        self._data_sink.close()
