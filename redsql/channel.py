"""
Defines facilities to manage channels transforming Redis input messages to SQL statements
"""
import logging
import threading
from typing import Optional, Dict, Any, List

import redis
import sqlalchemy as sql

import redsql.steps.abc.step as abstract_step


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
            self._redis_client.xgroup_create(name=self._stream_name, groupname=self._group_name, mkstream=True)
        else:
            self._logger.info(f"Try createing Redis group {self._group_name} on existing stream {self._stream_name}")
            try:
                # We cannot easily determine whether a group is already existing. Hence, try to create it and re-raise
                # the error in case it is not the expected one. (Thx to Denis and CLUE Data Sync.)
                self._redis_client.xgroup_create(name=self._stream_name, groupname=self._group_name)
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
        assert len(message) <= 1, "At most one message expected"
        if len(message) >= 1:
            assert message[0][0] == self._stream_name, "Received message from invalid stream"
            self._last_message_id = message[0][1][0][0]
            message_content = message[0][1][0][1]
        else:
            self._last_message_id = None
            message_content = None

        return message_content

    def ack_last_message(self):
        """Acknowledges the last message before retrieving the next one"""
        assert self._last_message_id is not None, "No message is left to acknowledge"
        self._redis_client.xack(self._stream_name, self._group_name, self._last_message_id)
        self._last_message_id = None


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

        self._transformation_steps: List[abstract_step.AbstractTransformationStep] = []

        self._sql_connection: Optional[sql.engine.Connection] = None
        self._sql_meta: Optional[sql.MetaData] = None

    def open(self, redis_pool: redis.ConnectionPool, sql_engine: sql.engine.Engine):
        """
        Opens the channel by initializing the Redis and DB client.

        The function has to be executed in the target thread that executes execute_channel_once()

        :param redis_pool: The possibly shared redis connection pool
        :param sql_engine: The possibly shared DB engine to create the connection from
        """

        self._data_source = _RedisStreamSource(self._config["trigger"], redis_pool, self._channel_name)

        self._logger.debug(f"Try to access meta data from the SQL engine {sql_engine}")
        self._sql_connection = sql_engine.connect()
        self._sql_meta = sql.MetaData()
        self._sql_meta.reflect(bind=self._sql_connection)
        self._logger.debug(f"SQL schema successfully retrieved.")

        self._logger.debug(f"Initialize external resources on all {len(self._transformation_steps)} steps.")
        for step in self._transformation_steps:
            step.open(sql_connection=self._sql_connection, sql_engine=sql_engine, redis_pool=redis_pool)

    def execute_channel_once(self):
        """
        Executes the channel once and returns.

        In case no message is received, the function will run into a timeout and return without executing the pipeline.
        """

        assert self._data_source is not None, "Need to open the channel before"

        message = self._data_source.get_next_message()
        if message is not None:

            output_messages = [message]
            for step in self._transformation_steps:
                output_messages = step.transform_messages(output_messages)
            # TODO: Write the messages to the DB
            self._data_source.ack_last_message()

    def close(self):
        """
        Closes the Redis and DB connection.
        """

        assert self._sql_connection is not None, "No SQL connection found. Call open(...) beforehand."

        for step in self._transformation_steps:
            step.close()

        self._sql_connection.close()
        self._sql_connection = None
