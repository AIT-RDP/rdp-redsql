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
