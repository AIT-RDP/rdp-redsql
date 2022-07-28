"""
Implements the RedSQL exceptions
"""
from typing import Dict, Any, Optional


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
