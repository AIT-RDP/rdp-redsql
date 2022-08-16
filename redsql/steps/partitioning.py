"""
Processing steps that split one message into several outputs
"""
import logging
from typing import Dict, Any, Iterable

import redsql.exc as exc
import redsql.steps.abc.step as abstract_step


class SplitByKey(abstract_step.AbstractOneToManyStep):
    """
    Transformation step that creates one unified message per key

    The step allows to define a set of keys that will be copied to every output message without splitting the input
    message. Additionally, renaming operations will be performed to unify the output representation.

    To ease debugging and testing, the output messages will be sorted by the source output key value, i.e. the original
    key.
    """

    def __init__(self, config: dict, channel_name: str, step_name: str, **kwargs):
        """
        :param config: The user configuration describing the decomposition step.
        :param channel_name: The name of the channel that contains the step
        :param step_name: A unique name of the processing step to simplify debugging
        :param kwargs: The dynamically added rest of any injected parameters
        """
        super(SplitByKey, self).__init__(**kwargs)

        self._logger = logging.getLogger(f"{__name__}.{channel_name}.{step_name}")
        self._always_include = config.get("always include", [])
        if not isinstance(self._always_include, list):
            raise ValueError(f"The 'always include' configuration of step {step_name} in {channel_name} must be a "
                             f"list but a {type(self._always_include)} is given.")

        self._destination_key = config["destination key"]
        self._source_key = config.get("source output key", "_source")

        self._logger.debug("Initialized SplitByKey operation: Generate a new message for each input key except "
                           f"{self._always_include} and rename the key to '{self._destination_key}'. "
                           f"The old key name will be available in '{self._source_key}'.")

    def transform_single_message(self, message: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        """Conducts the transformation and returns the split output messages"""

        include_keys = set(message.keys()).intersection(self._always_include)
        include_message = {key: message[key] for key in include_keys}

        split_keys = sorted(set(message.keys()).difference(self._always_include))
        split_messages = map(lambda key: {
            self._destination_key: message[key],
            self._source_key: key,
            **include_message
        }, split_keys)

        return split_messages


class UnpackArrayValues(abstract_step.AbstractOneToManyStep):
    """
    Splits arrays of values into multiple messages with just one value each key.
    """

    def __init__(self, config: dict, channel_name: str, step_name: str, **kwargs):
        """
        :param config: The user configuration describing the decomposition step.
        :param channel_name: The name of the channel that contains the step
        :param step_name: A unique name of the processing step to simplify debugging
        :param kwargs: The dynamically added rest of any injected parameters
        """

        self._unpack_keys = config["unpack keys"]
        if not isinstance(self._unpack_keys, list):
            raise ValueError(f"The 'unpack keys' configuration of step {step_name} in {channel_name} must be a "
                             f"list but a {type(self._always_include)} is given.")

    def transform_single_message(self, message: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        """
        Splits the arrays named by the 'unpack keys' configuration directives into multiple messages

        The i-th message will hold the i-th array values of all keys named by 'unpack keys'. Hence, it is assumed that
        all keys hold lists of the same size. Unrelated message keys will be copied to each output message without
        modification.

        :param message: The single input message
        :return: An iterable of output messages
        """

        missing_keys = set(self._unpack_keys).difference(message.keys())
        if len(missing_keys) > 0:
            raise exc.MessageFormatError(f"Missing keys to unpack: {missing_keys}", triggering_message=message)

        source_arrays = {key: message[key] for key in self._unpack_keys}
        if not all(isinstance(src, list) for src in source_arrays.values()):
            raise exc.MessageFormatError(f"Some source arrays are ({self._unpack_keys}) not encoded as lists",
                                         triggering_message=message)

        element_number = len(source_arrays[self._unpack_keys[0]]) if len(self._unpack_keys) > 0 else 0
        if not all(len(src) == element_number for src in source_arrays.values()):
            raise exc.MessageFormatError(f"Some arrays to unpack do not have {element_number} elements.",
                                         triggering_message=message)

        split_messages = map(lambda i: {
            **message,
            **{key: message[key][i] for key in source_arrays}
        }, range(element_number))
        return split_messages
