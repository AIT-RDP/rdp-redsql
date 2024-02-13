"""
Defines processing steps that create more complex data structures out of simpler ones.

The steps are mainly intended to cover complex SQL types such as JSON/JSONb objects but may be useful for other purposes
as well.
"""
import copy
from typing import Dict, Any

import redsql.steps.abc.step as abstract_step


class PackMessageValues(abstract_step.AbstractOneToOneStep):

    def __init__(self, config: dict, channel_name: str, step_name: str, **kwargs):
        """
        :param config: The user configuration describing the query step.
        :param channel_name: The name of the channel that contains the step
        :param step_name: A unique name of the processing step to simplify debugging
        :param kwargs: The dynamically added rest of any injected parameters
        """
        super(PackMessageValues, self).__init__(**kwargs)

        self._step_name = step_name
        self._channel_name = channel_name

        if "destination" not in config:
            raise KeyError(f"The PackMessageValues step {channel_name}/{step_name} requires a 'destination' "
                           " configuration that specifies the message structures.")
        self._destination_template = config["destination"]

        if not isinstance(self._destination_template, dict):
            raise TypeError(f"The 'destination' configuration of {channel_name}/{step_name} must be a dictionary of "
                            f"message keys. Got a {type(self._destination_template).__name__} instead.")

        if any(not isinstance(k, str) for k in self._destination_template.keys()):
            raise TypeError(f"The message keys (first nesting) of {channel_name}/{step_name} must be strings.")

    def transform_single_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Appends the message entries defined in the destination template and returns the newly created message

        :param message: The input message to extend
        :return: The extended message
        """

        return dict(**message, **self._replace_references(self._destination_template, message))

    def _replace_references(self, template, message: Dict[str, Any]):
        """Recursively replaces the references and returns a newly created message"""

        if isinstance(template, str) and template.startswith("%%"):
            ret = template[1:]  # Trim the first %%
        elif isinstance(template, str) and template.startswith("%"):
            # Resolve the message key
            message_key = template[1:]
            if message_key not in message:
                raise KeyError(f"The 'destination' template of {self._channel_name}/{self._step_name} references the "
                               f"message key '{message_key}' but no such key is found. Got only "
                               f"{list(message.keys())}.")
            ret = message[message_key]
        elif isinstance(template, dict):
            # Recursively process dicts
            ret = {
                self._replace_references(k, message): self._replace_references(v, message)
                for k, v in template.items()
            }
        elif isinstance(template, list):
            # Recursively process lists
            ret = [self._replace_references(v, message) for v in template]
        else:
            # Just pass on the value
            ret = copy.deepcopy(template)

        return ret
