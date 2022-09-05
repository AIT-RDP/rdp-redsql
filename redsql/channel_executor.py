"""
Implements the actual execution logic managing the individual channels
"""
import logging
import threading
from typing import Optional, Dict, Type, List

import redis
import sqlalchemy.engine

import redsql.channel as channel

logger = logging.getLogger(__name__)


class ThreadChannelExecutor:
    """Executes the hosted channel in a dedicated thread"""

    def __init__(self, channel_config: dict, redis_pool: redis.ConnectionPool, sql_engine: sqlalchemy.engine.Engine,
                 channel_name: str = "<channel>", ext_channel: Optional[channel.Channel] = None):
        """
        Initializes the channel executor.

        :param channel_config: The channel-specific configuration stanza
        :param redis_pool: The shared Redis pool to draw the connections from
        :param sql_engine: The SQL engine to draw the sql connections from
        :param channel_name: The name of the channel for debugging purpose
        :param ext_channel: An optional external channel to execute. The parameter is mainly intended for testing
            purpose. In case None is given, a channel object will be created.
        """

        if ext_channel is None:
            ext_channel = channel.Channel(channel_config, channel_name)

        self._channel = ext_channel
        self._logger = logging.getLogger(f"{__name__}.{channel_name}")
        self._exit_event = threading.Event()
        self._thread = threading.Thread(target=self._run_channel)

        self._redis_pool = redis_pool
        self._sql_engine = sql_engine

    @property
    def channel(self) -> channel.Channel:
        """Returns the managed channel. (Mostly for testing purpose)"""
        if self._thread.is_alive():
            raise AttributeError("The channel is accessed while the local executor is already started")
        return self._channel

    def _run_channel(self):
        """Periodically executes the channel logic until a termination request is received"""

        self._channel.open(self._redis_pool, self._sql_engine)
        try:
            while not self._exit_event.is_set():
                self._channel.execute_channel_once()
                self._flush_loggers()
        except Exception as err:
            self._logger.error(f"Caught a {type(err).__name__}: {err}")
            raise err
        finally:
            self._channel.close()

    def _flush_loggers(self):
        """Flushes the loggers to see immediate outputs"""
        for handler in self._logger.handlers:
            handler.flush()

    def start(self):
        """
        Starts the executor operation in an independent thread.
        """

        assert not self._exit_event.is_set(), "The executor has already been stopped"
        self._thread.start()

    def stop(self):
        """
        Signals to stop the executor but does not wait until it is actually stopped.

        The stop-join functions were split in order to allow a parallel stop
        """
        assert not self._exit_event.is_set(), "The executor has already been stopped"
        self._exit_event.set()

    def join(self):
        """
        Waits until the executor is stopped
        """
        assert self._exit_event.is_set(), "The executor was not stopped before"
        self._thread.join()

    def is_alive(self) -> bool:
        """Checks and returns the health status of the executor"""

        return self._thread.is_alive()


class ChannelSupervisor:
    """
    Manages a collection of channels and controls their operation

    The supervisor additionally provides functions to periodically check the operation of managed executors and restart
    an executor, if necessary.
    """

    def __init__(self, channels_config: Dict[str, dict], redis_pool: redis.ConnectionPool,
                 sql_engine: sqlalchemy.engine.Engine, ext_channels: Optional[Dict[str, channel.Channel]] = None):
        """
        Instantiates the channels and connected executors

        :param channels_config: The dictionary of individual channel configurations indexed by their names
        :param redis_pool: The redis pool to pass on to each channel
        :param sql_engine: The sql engine to pass on to each channel
        :param ext_channels: Some externally supplied channels to test the function
        """

        self._channels_config = channels_config
        self._redis_pool = redis_pool
        self._sql_engine = sql_engine

        if ext_channels is None:
            ext_channels = {}
        self._ext_channels = ext_channels

        self._channel_executors: Dict[str, ThreadChannelExecutor] = {
            ex_name: ThreadChannelExecutor(cnf, redis_pool, sql_engine, ex_name,
                                           ext_channel=ext_channels.get(ex_name, None))
            for ex_name, cnf in channels_config.items()
        }

    @property
    def channel_names(self) -> List[str]:
        """Returns the names of the channels for debugging purposes"""
        return list(self._channels_config.keys())

    def start(self):
        """Starts all channel executors"""

        for ex in self._channel_executors.values():
            ex.start()

    def stop(self):
        """Stops the operation of all channel executros and waits until all threads are stopped"""

        for ex in self._channel_executors.values():
            ex.stop()

        for ex in self._channel_executors.values():
            ex.join()

    def heartbeat(self) -> dict:
        """
        Executes the check and repair policy of the channel

        It is advised to periodically call the function to query the execution status and restart and failing executor

        :return Status descriptions of all channels
        """

        status = {}

        for ex_name, ex_channel in self._channel_executors.copy().items():
            if not ex_channel.is_alive():
                logger.warning(f"Found channel {ex_name} to be dead. Restart the channel now.")
                chn = ThreadChannelExecutor(self._channels_config[ex_name], self._redis_pool, self._sql_engine, ex_name,
                                            ext_channel=self._ext_channels.get(ex_name, None))
                chn.start()
                self._channel_executors[ex_name] = chn
                status[ex_name] = "restarted"
            else:
                status[ex_name] = "ok"

        return status
