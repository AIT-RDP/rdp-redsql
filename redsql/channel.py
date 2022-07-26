"""
Defines facilities to manage channels transforming Redis input messages to SQL statements
"""
import logging
import threading
from typing import Optional

import redis
import sqlalchemy as sql


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

        self._redis_client = None
        self._sql_connection: Optional[sql.engine.Connection] = None
        self._sql_meta: Optional[sql.MetaData] = None

    def open(self, redis_pool: redis.ConnectionPool, sql_engine: sql.engine.Engine):
        """
        Opens the channel by initializing the Redis and DB client.

        The function has to be executed in the target thread that executes execute_channel_once()

        :param redis_pool: The possibly shared redis connection pool
        :param sql_engine: The possibly shared DB engine to create the connection from
        """

        self._logger.debug(f"Try to access the Redis instance via {redis_pool}")
        self._redis_client = redis.Redis(connection_pool=redis_pool)
        self._redis_client.ping()
        self._logger.debug(f"Successfully connected to Redis")

        self._logger.debug(f"Try to access meta data from the SQL engine {sql_engine}")
        self._sql_connection = sql_engine.connect()
        self._sql_meta = sql.MetaData()
        self._sql_meta.reflect(bind=self._sql_connection)
        self._logger.debug(f"SQL schema successfully retrieved.")

    def execute_channel_once(self):
        """
        Executes the channel once and returns.

        In case no message is received, the function will run into a timeout and return without executing the pipeline.
        """

        pass  # TODO: Implement

    def close(self):
        """
        Closes the Redis and DB connection.
        """

        assert self._sql_connection is not None, "No SQL connection found. Call open(...) beforehand."

        self._sql_connection.close()
        self._sql_connection = None
