"""
Tests the transformation steps
"""

import pytest

import redsql.steps.transformation as tr
import redsql.exc as exc


@pytest.mark.parametrize("test_value,data_type", [
    (1., "double"),
    (0., "double"),
    (-2.5, "double"),
    (0, "bigint"),
    (1, "bigint"),
    (-42, "bigint"),
    (True, "boolean"),
    (False, "boolean"),
    ("hello", "jsonb"),
    ({"hello": "types"}, "jsonb"),
    ([1, 2, 3], "jsonb")
])
def test_resolve_type_minimal_config(test_value, data_type):
    """Tests the type resolution method with a minimal config"""

    config = {}
    step = tr.ResolveDataType(config, "test-channel", "test-step")

    in_message = {"value": test_value, "unchanged": 123}
    out_messages = step.transform_messages([in_message])
    out_messages = list(out_messages)

    assert len(out_messages) == 1
    assert out_messages[0] == {
        "value": test_value,
        "unchanged": 123,
        "data_type": data_type
    }


def test_resolve_type_joint_key():
    """Tests the type resolution step using a joint key"""

    config = {
        "source": [
            {"type_of": "val"},
            {"value_of": "na"},
        ],
        "lookup_table": {
            ("float", "power"): "float.power",
            ("bool", "power"): "bool.power",
            ("float", "energy"): "float.energy",
            ("bool", "energy"): "bool.energy",
            "_default": "everything"
        },
        "output_key": "dst"
    }
    step = tr.ResolveDataType(config, "test-channel", "test-step")

    in_messages = [
        {"na": "power", "val": 2.},
        {"na": "power", "val": True},
        {"na": "energy", "val": 3.},
        {"na": "energy", "val": False},
        {"na": "answer", "val": 42},
    ]
    out_messages = step.transform_messages(in_messages)
    out_messages = list(out_messages)

    assert len(out_messages) == 5
    assert out_messages[0] == {
        "na": "power", "val": 2.,
        "dst": "float.power"
    }
    assert out_messages[1] == {
        "na": "power", "val": True,
        "dst": "bool.power"
    }
    assert out_messages[2] == {
        "na": "energy", "val": 3.,
        "dst": "float.energy"
    }
    assert out_messages[3] == {
        "na": "energy", "val": False,
        "dst": "bool.energy"
    }
    assert out_messages[4] == {
        "na": "answer", "val": 42,
        "dst": "everything"
    }


def test_resolve_type_omit_message():
    """Tests the type resolution on setting no dedicated message"""

    config = {
        "source": [
            {"value_of": "na"},
        ],
        "lookup_table": {
            "power": "float.power",
        },
        "output_key": "dst",
        "default": "omit"  # Omit unknown values and do not set "dst"
    }
    step = tr.ResolveDataType(config, "test-channel", "test-step")

    in_messages = [
        {"na": "energy", "val": 2., "dst": "unchanged"},
    ]
    out_messages = step.transform_messages(in_messages)
    out_messages = list(out_messages)

    assert len(out_messages) == 1
    assert out_messages[0] == {
        "na": "energy", "val": 2.,
        "dst": "unchanged"
    }


def test_resolve_type_lookup_error():
    """Tests whether an error is raised, in case no omit or default is given"""

    config = {
        "source": [
            {"value_of": "na"},
        ],
        "lookup_table": {
            "power": "float.power",
        },
        "output_key": "dst",
    }
    step = tr.ResolveDataType(config, "test-channel", "test-step")

    in_messages = [
        {"na": "energy", "val": 2.},
    ]
    with pytest.raises(exc.MessageFormatError):
        out_messages = step.transform_messages(in_messages)
        list(out_messages)


def test_resolve_type_content_key_error():
    """Checks whether an error is raised for invalid content keys"""

    config = {
        "source": [
            {"value_of": "missing_key"},
        ],
        "lookup_table": {
            "power": "float.power",
        },
        "output_key": "dst",
    }
    step = tr.ResolveDataType(config, "test-channel", "test-step")

    in_messages = [
        {"na": "energy", "val": 2.},
    ]
    with pytest.raises(exc.MessageFormatError):
        out_messages = step.transform_messages(in_messages)
        list(out_messages)


def test_resolve_type_type_key_error():
    """Checks whether an error is raised for invalid content keys"""

    config = {
        "source": [
            {"type_of": "missing_key"},
        ],
        "lookup_table": {
            "power": "float.power",
        },
        "output_key": "dst",
    }
    step = tr.ResolveDataType(config, "test-channel", "test-step")

    in_messages = [
        {"na": "energy", "val": 2.},
    ]
    with pytest.raises(exc.MessageFormatError):
        out_messages = step.transform_messages(in_messages)
        list(out_messages)


def test_resolve_type_integrated_config():
    """Tests the configuration in case integration keys are present"""

    config = {
        "type": "ResolveDataType",  # Needed for dynamic step instantiation
    }
    tr.ResolveDataType(config, "test-channel", "test-step")
