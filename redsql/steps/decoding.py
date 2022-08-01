"""
Implements the message decoding and some basic type checking rules

The decoding logic is provided by an AbstractTransformationStep to unity the processing pipeline. Nevertheless, the
class itself may be statically instantiated to improve the readability of the configuration section.
"""
import datetime
import json
import logging
from typing import Dict, Any, Callable

import dateutil.parser

import redsql.exc as exc
import redsql.steps.abc.step as abstract_step


class DecodingStep(abstract_step.AbstractOneToOneStep):
    """Individually decodes each message field according to the user configuration"""

    def __init__(self, config: dict, channel_name: str, step_name: str, **kwargs):
        """
        :param config: The user configuration describing the encoding rules. For each message key, a dedicated decoder
            must be specified. A default key "_default" may be used to specify the default decoder.
        :param channel_name: The name of the channel that contains the step
        :param step_name: A unique name of the processing step to simplify debugging
        :param kwargs: The dynamically added rest of any injected parameters
        """
        super(DecodingStep, self).__init__(**kwargs)

        self._logger = logging.getLogger(f"{__name__}.{channel_name}.{step_name}")
        self._decoders = self._create_decoders(config)

    def _create_decoders(self, config: dict) -> Dict[str, Callable]:
        """
        Creates a dict of decoders that maps message keys to the corresponding decoder

        :param config: The decoder-specific configuration stanza
        """

        available_decoders = {
            "KeepEncoding": lambda x: x,
            "JSON": self._decode_json,
            "JSONDatetimeString": self._decode_json_iso_datetime,
        }

        message_decoders = {}
        for message_key, decoder_name in config.items():
            if decoder_name not in available_decoders:
                raise KeyError(f"The configured encoding '{decoder_name}' for message key '{message_key}' is unknown."
                               f" Available encodings: {list(available_decoders.keys())}")
            message_decoders[message_key] = available_decoders[decoder_name]

        if "_default" not in message_decoders:
            default_encoder = "KeepEncoding"
            message_decoders["_default"] = available_decoders[default_encoder]
            self._logger.debug(f"Registered default encoding ({default_encoder})")

        return message_decoders

    @staticmethod
    def _decode_json(value: str) -> Any:
        """Decodes the JSON string and returns its content"""
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise exc.MessageFormatError(f"Invalid JSON string found: {value}") from e

    @staticmethod
    def _decode_json_iso_datetime(value: str) -> datetime.datetime:
        """Decodes the JSON string into a datetime"""
        value = DecodingStep._decode_json(value)
        try:
            return dateutil.parser.isoparse(value)
        except dateutil.parser.ParserError as e:
            raise exc.MessageFormatError(f"Invalid ISO Timestamp: {value}") from e

    def transform_single_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms the input message using the configured decoding rules and returns the result."""

        default_decoder = self._decoders["_default"]
        decoded_message = {}
        for message_key, raw_value in message.items():
            try:
                decoded_message[message_key] = self._decoders.get(message_key, default_decoder)(raw_value)
            except exc.MessageFormatError as e:
                e.triggering_message = message
                raise e
        return decoded_message
