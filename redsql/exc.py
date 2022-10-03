"""
Implements the RedSQL exceptions
"""
import datetime
import json
from typing import Dict, Any, Optional

import pandas as pd


class MessageFormatError(ValueError):
    """
    Indicates a malformed message.

    The error indicates an invalid message that cannot be processed even if the process is repeated.
    """

    def __init__(self, description: str, triggering_message: Optional[Dict[str, Any]] = None,
                 external_message: Optional[Dict[str, Any]] = None, **kwargs):
        """
        Initializes the error

        :param description: A descriptive error message that can be displayed to a user
        :param triggering_message: The intermediate message that triggers the error
        :param external_message: The original, unprocessed message as received by the data source
        """

        super(MessageFormatError, self).__init__(description, **kwargs)

        self.description = description
        self.triggering_message = triggering_message
        self.external_message = external_message

    class _ExtJSONEncoder(json.JSONEncoder):
        """Returns an extended string representation"""

        def default(self, o):
            if isinstance(o, pd.Timestamp) or isinstance(o, datetime.datetime):
                return o.isoformat()
            else:
                return json.JSONEncoder.default(self, o)

    def get_triggering_message_string(self) -> str:
        """Returns a string representation of the triggering message"""
        return json.dumps(self.triggering_message, cls=self._ExtJSONEncoder, indent=2)

    def get_external_message_string(self) -> str:
        """Returns a string representation of the externally received message"""
        return json.dumps(self.external_message, cls=self._ExtJSONEncoder, indent=2)
