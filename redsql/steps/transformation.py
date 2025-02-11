"""
Processing steps that transform basic values to other values
"""
import copy
import enum
from typing import Dict, Any, Union, List

import pydantic

import redsql.steps.abc.step as abstract_step
import redsql.exc as exc


class TypeSource(pydantic.BaseModel):
    """Defines a source config referencing a particular type"""
    type_of: str = pydantic.Field(description="The message field key to extract the type information")


class ContentSource(pydantic.BaseModel):
    """Defines a source config referencing a particular variable value"""
    content_of: str = pydantic.Field(description="The message field key to directly extract the value")


SourceEntryType = Union[TypeSource, ContentSource]


class DefaultOptions(str, enum.Enum):
    """Options how to deal with values that could not be resolved"""
    omit = "omit"
    lookup = "lookup"


# The default type associations
_default_lookup_table = {
    "float": "double",
    "int": "bigint",
    "bool": "boolean",
    "str": "jsonb",
    "dict": "jsonb",
    "list": "jsonb"
}


class ResolveDataTypeConfig(pydantic.BaseModel):
    """Defines the configuration of the type resolution step"""

    source: Union[List[SourceEntryType], str] = pydantic.Field(
        description="The source configuration determining the value that is translated",
        default=[TypeSource(type_of="value")]
    )
    lookup_table: Dict[Union[tuple, str], str] = pydantic.Field(
        description="The actual lookup table that maps the source to the destination value",
        default=_default_lookup_table
    )
    output_key: str = pydantic.Field(
        description="The key to write the resolved type in.",
        default="data_type"
    )
    default: DefaultOptions = pydantic.Field(
        description="The behaviour in case the source is not listed.",
        default=DefaultOptions.lookup
    )


class ResolveDataType(abstract_step.AbstractOneToOneStep):
    """
    Implements a type lookup logic including static assignments
    """

    def __init__(self, config: dict, channel_name: str, step_name: str, **kwargs):
        """
        :param config: The user configuration describing the decomposition step.
        :param channel_name: The name of the channel that contains the step
        :param step_name: A unique name of the processing step to simplify debugging
        :param kwargs: The dynamically added rest of any injected parameters
        """
        super().__init__(**kwargs)

        config = ResolveDataTypeConfig.model_validate(config)
        # Normalize the configuration
        if isinstance(config.source, str):
            config.source = [TypeSource(type_of=config.source)]

        config.lookup_table = {
            ((key,) if not isinstance(key, tuple) else key): val
            for key, val in config.lookup_table.items()
        }

        self._config = config
        self._channel_name = channel_name
        self._step_name = step_name

    def transform_single_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Performs the lookup transformation on the particular message"""
        message = copy.copy(message)
        lookup_tab = self._config.lookup_table

        key = self._extract_lookup_key(message)
        if key in lookup_tab:
            message[self._config.output_key] = lookup_tab[key]
        elif self._config.default == DefaultOptions.lookup and ("_default",) in lookup_tab:
            message[self._config.output_key] = lookup_tab[("_default",)]
        elif self._config.default == DefaultOptions.lookup:
            # Default entry not given
            raise exc.MessageFormatError(
                f"Cannot look up the key {key} in the lookup table of {self._channel_name}.{self._step_name}. Neither "
                f"the key nor a '_default' value are present.",
                triggering_message=message
            )

        return message

    def _extract_lookup_key(self, message: Dict[str, Any]) -> tuple:
        """Extracts the lookup key from the message"""
        ret = []
        for i, source in enumerate(self._config.source):
            if isinstance(source, TypeSource):
                ret.append(self._extract_type_source(source, i, message))
            elif isinstance(source, ContentSource):
                ret.append(self._extract_content_source(source, i, message))
            else:
                assert False, "Unknown source type found. Consider opening a ticket, if you encounter this."

        return tuple(ret)

    def _extract_type_source(self, source: TypeSource, index: int, message: Dict[str, Any]):
        """Extracts the type source and returns it"""

        if source.type_of not in message:
            raise exc.MessageFormatError(
                f"Message key '{source.type_of}' of {index}-th source of "
                f"{self._channel_name}.{self._step_name} not found.",
                triggering_message=message
            )

        value = message[source.type_of]
        if value is None:
            return None

        module = type(value).__module__
        name = type(value).__name__

        if module != "builtins":
            return f"{module}.{name}"
        else:
            return name

    def _extract_content_source(self, source: ContentSource, index: int, message: Dict[str, Any]):
        """Extracts the type source and returns it"""

        if source.content_of not in message:
            raise exc.MessageFormatError(
                f"Message key '{source.content_of}' of {index}-th source of "
                f"{self._channel_name}.{self._step_name} not found.",
                triggering_message=message
            )

        value = message[source.content_of]
        return value
