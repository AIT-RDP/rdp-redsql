"""
Implements the actual execution logic managing the individual channels
"""
import logging
import threading
from typing import Optional

import redsql.channel as channel


class ThreadChannelExecutor:
    """Executes the hosted channel in a dedicated thread"""

    def __init__(self, channel_config: dict, channel_name: str = "<channel>",
                 ext_channel: Optional[channel.Channel] = None):
        """
        Initializes the channel executor.

        :param channel_config: The channel-specific configuration stanza
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

    @property
    def channel(self) -> channel.Channel:
        """Returns the managed channel. (Mostly for testing purpose)"""
        if self._thread.is_alive():
            raise AttributeError("The channel is accessed while the local executor is already started")
        return self._channel

    def _run_channel(self):
        """Periodically executes the channel logic until a termination request is received"""

        while not self._exit_event.is_set():
            self._channel.execute_channel_once()

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
