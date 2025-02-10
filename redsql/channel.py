"""
Defines facilities to manage channels transforming Redis input messages to SQL statements
"""
import importlib
import inspect
import logging
from typing import Optional, Dict, Any, List

import prometheus_client as prom
import redis
import sqlalchemy as sql

import redsql.steps.abc.step as abstract_step
import redsql.steps.decoding as decoding_step
import redsql.exc as exc
from redsql.sql_sink import SQLTableSink


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
        self._trim_cnt = 0  # Counts the number of messages since the last trim operation
        self._trim_max = config.get("trim length", None)

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

        self._trim_cnt += 1 if self._trim_max is not None else 0
        if self._trim_max is not None and self._trim_cnt >= (self._trim_max / 5.0):  # Permit 20% overshoot
            trim_cnt = self._redis_client.xtrim(self._stream_name, self._trim_max, approximate=True)
            self._trim_cnt = 0
            self._logger.debug(f"Trimmed the Redis stream to {self._trim_max} elements removing {trim_cnt} entries.")


class Channel:
    """A data pipeline with a unified set of processing steps"""

    _prom_message_cnt = prom.Counter("redsql_processed_messages", labelnames=["channel_name", "type"],
                                     documentation="Message counts for each channel")

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
        self._data_sink: Optional[SQLTableSink] = None

        self._transformation_steps: List[abstract_step.AbstractTransformationStep] = [
            decoding_step.DecodingStep(config=channel_config.get("encoding", {}), channel_name=channel_name,
                                       step_name="0-decoding")
        ]

        step_config = channel_config.get("steps", [])
        self._transformation_steps += self._instantiate_steps(step_config, channel_name, self._logger)

        for tp_name in ["in", "success", "err_general", "err_format"]:
            self._prom_message_cnt.labels(channel_name=channel_name, type=tp_name)

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
        self._data_sink = SQLTableSink(self._config["data sink"], sql_engine, self._channel_name)

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
            batch_size = len(output_messages)
            self._prom_message_cnt.labels(channel_name=self._channel_name, type="in").inc(batch_size)

            try:
                # Run the processing steps and push the result
                for step in self._transformation_steps:
                    output_messages = step.transform_messages(output_messages)
                self._data_sink.insert_messages(output_messages)
                self._prom_message_cnt.labels(channel_name=self._channel_name, type="success").inc(batch_size)

            except exc.MessageFormatError as e:
                self._data_source.ack_last_message()  # Permanent error. Remove the message from the queue
                e.external_message = message
                self._prom_message_cnt.labels(channel_name=self._channel_name, type="err_format").inc(batch_size)
                raise
            except Exception as e:
                self._prom_message_cnt.labels(channel_name=self._channel_name, type="err_general").inc(batch_size)
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
