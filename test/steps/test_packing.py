"""
Tests the packing functions
"""
from typing import Dict, Any

import pytest

import redsql.steps.packing as packing


@pytest.fixture()
def packing_config() -> Dict[str, Any]:
    """Returns an exemplary configr for the packing step"""
    return {
        "destination": {
            "simple_message": "%name",
            "complex_message": [
                {"id": 0, "position": "first%", "my_unit": "%unit"},
                {"id": 1, "position": "%%second", "my_role": "%role"}
            ]
        }
    }


def test_pack_message_values_nested(packing_config):
    """Tests the standard operation of a nested value packing"""

    step = packing.PackMessageValues(packing_config, "<test-channel>", "<test-step>")

    messages = list(step.transform_messages([
        {"name": "Legion", "unit": "many", "role": 666}
    ]))

    assert len(messages) == 1
    assert messages[0] == {
        "name": "Legion",
        "unit": "many",
        "role": 666,
        "simple_message": "Legion",
        "complex_message": [
            {"id": 0, "position": "first%", "my_unit": "many"},
            {"id": 1, "position": "%second", "my_role": 666}
        ]
    }


def test_pack_message_values_invalid_config_types():
    """Tests the configuration assessment of the PackMessageValues"""

    # There must be a destination directive
    with pytest.raises(KeyError) as err_handler:
        packing.PackMessageValues({
            "no-destination": {}
        }, "<test-channel>", "<test-step>")
    assert "destination" in str(err_handler.value)

    # destination need to be a dictionary
    with pytest.raises(TypeError) as err_handler:
        packing.PackMessageValues({
            "destination": ["no-dict"]
        }, "<test-channel>", "<test-step>")
    assert "dict" in str(err_handler.value)

    # Message keys must be string
    with pytest.raises(TypeError) as err_handler:
        packing.PackMessageValues({
            "destination": {
                42: "no-string-key"
            }
        }, "<test-channel>", "<test-step>")
    assert "str" in str(err_handler.value)
