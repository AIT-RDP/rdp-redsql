"""
Tests the partitioning transformation steps
"""

import redsql.steps.partitioning as partitioning


def test_split_by_key_minimal():
    """Tests the minimal configuration of SplitByKey"""

    step = partitioning.SplitByKey(config={
        "destination key": "val"
    }, channel_name="<test>", step_name="<test>")

    input_messages = [{
        "temperature": 27.9,
        "humidity": 64.0
    }]

    step.open()
    output_messages = list(step.transform_messages(input_messages))
    step.close()

    reference_message = [{
        "val": 64.0,
        "_source": "humidity"
    }, {
        "val": 27.9,
        "_source": "temperature"
    }]
    assert output_messages == reference_message


def test_split_by_key_extensive():
    """Tests the minimal configuration of SplitByKey"""

    step = partitioning.SplitByKey(config={
        "destination key": "val",
        "always include": ["dp id"],
        "source output key": "type"
    }, channel_name="<test>", step_name="<test>")

    input_messages = [{
        "temperature": 27.9,
        "humidity": 64.0,
        "dp id": 1
    }]

    step.open()
    output_messages = list(step.transform_messages(input_messages))
    step.close()

    reference_message = [{
        "val": 64.0,
        "type": "humidity",
        "dp id": 1
    }, {
        "val": 27.9,
        "type": "temperature",
        "dp id": 1
    }]
    assert output_messages == reference_message


def test_unpack_array_values():
    """Tests the unpack step"""

    step = partitioning.UnpackArrayValues({"unpack keys": ["arr_a", "arr_b"]}, channel_name="<>", step_name="<>")

    input_messages = [
        {"arr_a": ["one", "two"], "arr_b": [15, 23], "meta": "lab"},
        {"arr_a": [], "arr_b": [], "meta": "void"},
    ]

    step.open()
    output_messages = list(step.transform_messages(input_messages))
    step.close()

    reference_messages = [
        {"arr_a": "one", "arr_b": 15, "meta": "lab"},
        {"arr_a": "two", "arr_b": 23, "meta": "lab"}
    ]
    assert output_messages == reference_messages


def test_unpack_array_values_single_empty():
    """Tests the unpack function if an empty array is given"""

    step = partitioning.UnpackArrayValues({"unpack keys": ["arr_a", "arr_b"]}, channel_name="<>", step_name="<>")
    input_messages = [
        {"arr_a": [], "arr_b": [], "meta": "void"},
    ]

    step.open()
    output_messages = list(step.transform_messages(input_messages))
    step.close()

    assert len(output_messages) == 0
