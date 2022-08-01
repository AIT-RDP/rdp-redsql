"""
Tests the decoding step in detail
"""
import datetime

import pytest

import redsql.steps.decoding as decoding
import redsql.exc as exc

utc = datetime.timezone.utc
utcp1 = datetime.timezone(datetime.timedelta(hours=1), name="UTC+1")


@pytest.fixture()
def decoding_step() -> decoding.DecodingStep:
    """Returns a preconfigured and initialized decoding step"""

    dec = decoding.DecodingStep(config={
        "_default": "JSON",
        "json_value": "JSON",
        "dt_value": "DatetimeString",
        "dt_json_str_value": "JSONDatetimeString",
        "dt_json_list_value": "JSONListWithDatetimeStrings"
    }, channel_name="<test>", step_name="<test>")
    dec.open()
    yield dec
    dec.close()


@pytest.mark.parametrize("message,reference", [
    ({"my_default": "\"Pretty Standard\""}, {"my_default": "Pretty Standard"}),
    ({"json_value": "{\"a\": \"b\"}"}, {"json_value": {"a": "b"}}),
    ({"dt_value": "1970-01-01T00:00:00+00"}, {"dt_value": datetime.datetime(1970, 1, 1, tzinfo=utc)}),
    ({"dt_json_str_value": "\"2022-02-02T00:00:00+01\""},
     {"dt_json_str_value": datetime.datetime(2022, 2, 2, tzinfo=utcp1)}),
    ({"dt_json_list_value": "[\"2022-02-02T00:00:00+01\", \"2022-02-03T00:00:00+00\"]"},
     {"dt_json_list_value": [datetime.datetime(2022, 2, 2, tzinfo=utcp1), datetime.datetime(2022, 2, 3, tzinfo=utc)]}),
    ({"dt_json_list_value": "[]"}, {"dt_json_list_value": []}),
    ({"dt_value": "1970-01-02T00:00:00+00", "desc": "\"The day after history began\""},
     {"dt_value": datetime.datetime(1970, 1, 2, tzinfo=utc), "desc": "The day after history began"}),
])
def test_decoding(decoding_step, message, reference):
    """tests the decoding step using the message and its reference"""

    dec_messages = list(decoding_step.transform_messages([message]))
    assert len(dec_messages) == 1
    assert dec_messages[0] == reference


@pytest.mark.parametrize("message", [
    {"json_value": "\"until the end"},
    {"dt_value": "noon"},
    {"dt_json_str_value": "\"eight o'clock\""},
    {"dt_json_list_value": "{}"},
    {"dt_json_list_value": "[\"2022-02-02T00:00:00+01\", \"Ooops, I did it again\"]"}
])
def test_invalid_encoding(decoding_step, message):
    """tests whether an appropriate exception is thrown"""

    with pytest.raises(exc.MessageFormatError) as err_desc:
        list(decoding_step.transform_messages([message]))
    assert err_desc.value.triggering_message == message


def test_default_keep():
    """Tests the default behaviour in keeping the string value"""

    dec = decoding.DecodingStep(config={}, channel_name="<test>", step_name="<test>")
    dec.open()

    messages = list(dec.transform_messages([
        {
            "int_val": "-1",
            "list_val": ["yeah"]
        }
    ]))

    assert len(messages) == 1
    message = messages[0]

    assert isinstance(message["int_val"], str)
    assert message["int_val"] == "-1"
    assert isinstance(message["list_val"], list)
    assert message["list_val"] == ["yeah"]

    dec.close()
