"""
Processing steps that split one message into several outputs
"""
import logging
from typing import Dict, Any, Iterable

import redsql.steps.abc.step as abstract_step


class SplitByKey(abstract_step.AbstractOneToManyStep):
    """
    Transformation step that creates one unified message per key

    The step allows to define a set of keys that will be copied to every output message without splitting the input
    message. Additionally, renaming operations will be performed to unify the output representation.
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
        
        split_keys = set(message.keys()).difference(self._always_include)
        split_messages = map(lambda key: {
            self._destination_key: message[key],
            self._source_key: key,
            **include_message
        }, split_keys)

        return split_messages
