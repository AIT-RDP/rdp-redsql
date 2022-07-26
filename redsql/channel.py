"""
Defines facilities to manage channels transforming Redis input messages to SQL statements
"""
import logging
import threading


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

    def execute_channel_once(self):
        """
        Executes the channel once and returns.

        In case no message is received, the function will run into a timeout and return without executing the pipeline.
        """

        pass  # TODO: Implement