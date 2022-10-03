"""
Performs some rudimentary tests on the exception classes
"""
import datetime

import pandas as pd
import pytest

import redsql.exc as exc


@pytest.mark.parametrize("message,reference", [
    ({"time": datetime.datetime(2022, 10, 3, 0, 1, tzinfo=datetime.timezone.utc)},
     "{\n  \"time\": \"2022-10-03T00:01:00+00:00\"\n}"),
    ({"time": pd.Timestamp("2022-10-03T00:01:00+00:00")}, "{\n  \"time\": \"2022-10-03T00:01:00+00:00\"\n}"),
    ({"a": 42}, "{\n  \"a\": 42\n}"),
])
def test_message_format_error_external_message(message, reference):
    """Tests the external message representation"""

    err = exc.MessageFormatError("Test Error", external_message=message)
    assert err.description == "Test Error"
    assert err.get_external_message_string() == reference


@pytest.mark.parametrize("message,reference", [
    ({"time": datetime.datetime(2022, 10, 3, 0, 1, tzinfo=datetime.timezone.utc)},
     "{\n  \"time\": \"2022-10-03T00:01:00+00:00\"\n}"),
    ({"time": pd.Timestamp("2022-10-03T00:01:00+00:00")}, "{\n  \"time\": \"2022-10-03T00:01:00+00:00\"\n}"),
    ({"a": 42}, "{\n  \"a\": 42\n}"),
])
def test_message_format_error_triggering_message(message, reference):
    """Tests the external message representation"""

    err = exc.MessageFormatError("Test Error", triggering_message=message)
    assert err.description == "Test Error"
    assert err.get_triggering_message_string() == reference
