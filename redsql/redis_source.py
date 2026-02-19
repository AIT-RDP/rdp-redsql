"""
Implements a redis stream source that implements the stream management logic
"""
import logging
from typing import Optional, Dict, Any

import redis


class RedisStreamSource:
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
