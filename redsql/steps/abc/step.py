"""
Hosts the interface to the abstract processing step
"""

import abc
from typing import Iterable, Dict, Any


class AbstractTransformationStep(abc.ABC):
    """
    Defines a transformation step that transforms a stream ´(iterator) of messages into another stream

    The step will be initialized by passing a series of keyword arguments. To ensure maximum compatibility, it is
    advised to catch all keyword arguments. Currently, the following arguments are supported:
     * channel_name: The name of the channel for debugging reasons
     * step_name: A unique name of each step. Mostly for debugging, too
     * config: The step-specific configuration stanza
    """

    def __init__(self, **kwargs):
        pass

    @abc.abstractmethod
    def transform_messages(self, message_input: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """
        Transforms the input messages to an arbitrary series of output messages

        :param message_input: The series of input messages to process.
        :return: An iterable that may hold an arbitrary amount of output messages
        """

        pass

    def open(self, **kwargs):
        """
        Hook that may be used to initializes resources within the destination thread

        :param kwargs: The external resources passed on to the transformation step
        """
        pass

    def close(self):
        """
        Hook to inform the processing step to free additional resources
        """