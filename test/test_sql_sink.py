"""
Specifically assesses the SQL sink
"""

import pandas as pd
import pytest

import redsql.sql_sink as sink
import redsql.exc as exc


@pytest.fixture()
def multi_table_sink_config(test_table: str) -> dict:
    """Returns a simplified test config"""

    return {
        "table_key": "data_type",
        "tables": {
            "huge-int": {
                "table": test_table,
                "columns": {
                    "value_int": "val",
                    "dp_id": "dp_id",
                    "obs_time": "obs_time"
                }
            },
            "little.double": {
                "table": test_table,
                "columns": {
                    "value_float": "val",
                    "obs_time": "obs_time"
                }
            },
            "general@text": {
                "table": test_table,
                "columns": {
                    "value_text": "val"
                }
            }
        }
    }


def test_sql_sink_basic_multi_table_insert(sql_engine, multi_table_sink_config):
    """Tests the multi-tables capabilities of the sql sink"""

    table_sink = sink.SQLTableSink(multi_table_sink_config, sql_engine, "test-channel")
    messages = [
        # Test the standard data types
        {
            "data_type": "huge-int",
            "dp_id": 3,
            "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
            "val": 2
        },
        {
            "data_type": "little.double",
            "dp_id": 4,
            "obs_time": pd.to_datetime("2024-12-31T01:00:00Z"),
            "val": 1e6
        },
        {
            "data_type": "general@text",
            "dp_id": 5,
            "obs_time": pd.to_datetime("2024-12-31T02:00:00Z"),
            "val": "ok"
        },
    ]
    table_sink.insert_messages(messages)

    with sql_engine.connect() as con:
        data = pd.read_sql("""
                SELECT dp_id, obs_time, value_int, value_float, value_text FROM test_table ORDER BY dp_id
            """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [3, 4, 5],
        "obs_time": pd.to_datetime(["2024-12-31T00:00:00Z", "2024-12-31T01:00:00Z", "2024-12-31T02:00:00Z"]),
        "value_int": [2, 42, 42],
        "value_float": [None, 1e6, None],
        "value_text": ["Nothing to add", "Nothing to add", "ok"]
    }), check_names=False)


def test_sql_sink_basic_multi_table_default_insert(sql_engine, multi_table_sink_config):
    """Tests the multi-tables capabilities of the sql sink regarding default tables"""

    multi_table_sink_config["tables"]["_default"] = {
        "table": "test_table",
        "columns": {
            "value_text": "val"
        }
    }

    table_sink = sink.SQLTableSink(multi_table_sink_config, sql_engine, "test-channel")
    messages = [
        {
            "data_type": "a new type",
            "dp_id": 3,
            "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
            "val": "new"
        }
    ]
    table_sink.insert_messages(messages)

    with sql_engine.connect() as con:
        data = pd.read_sql("""
                SELECT dp_id, obs_time, value_int, value_float, value_text FROM test_table ORDER BY dp_id
            """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [3],
        "obs_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value_int": [42],
        "value_float": [None],
        "value_text": ["new"]
    }), check_names=False)


def test_sql_sink_type_conversion(sql_engine, multi_table_sink_config):
    """Tests the multi-tables capabilities of the sql sink"""

    table_sink = sink.SQLTableSink(multi_table_sink_config, sql_engine, "test-channel")
    messages = [
        # Test the type conversion capabilities
        {
            "data_type": "huge-int",
            "dp_id": 3,
            "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
            "val": 2.0
        },
        {
            "data_type": "little.double",
            "dp_id": 4,
            "obs_time": pd.to_datetime("2024-12-31T01:00:00Z"),
            "val": 4
        },
        {
            "data_type": "little.double",
            "dp_id": 5,
            "obs_time": pd.to_datetime("2024-12-31T02:00:00Z"),
            "val": True
        },
        {
            "data_type": "little.double",
            "dp_id": 6,
            "obs_time": pd.to_datetime("2024-12-31T03:00:00Z"),
            "val": None  # Test whether NULL inserts still work (despite type casting)
        },
        {
            "data_type": "general@text",
            "dp_id": 7,
            "obs_time": pd.to_datetime("2024-12-31T04:00:00Z"),
            "val": 43
        },
    ]
    table_sink.insert_messages(messages)

    with sql_engine.connect() as con:
        data = pd.read_sql("""
                SELECT dp_id, obs_time, value_int, value_float, value_text FROM test_table ORDER BY dp_id
            """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [3, 4, 5, 6, 7],
        "obs_time": pd.to_datetime([
            "2024-12-31T00:00:00Z", "2024-12-31T01:00:00Z", "2024-12-31T02:00:00Z", "2024-12-31T03:00:00Z",
            "2024-12-31T04:00:00Z"
        ]),
        "value_int": [2, 42, 42, 42, 42],
        "value_float": [None, 4., 1., None, None],
        "value_text": ["Nothing to add"] * 4 + ["43"]
    }), check_names=False)


@pytest.mark.parametrize("additional_key,additional_value", [
    ("table", "test_table"),
    ("columns", {}),
    ("update duplicate values", True)
])
def test_sql_sink_invalid_keys(sql_engine, additional_key, additional_value):
    """Tests the sink with invalid (additional) legacy config keys"""

    config = {
        "tables": {
            "_default": {
                "table": "test_table",
                "columns": {}
            }
        },
        additional_key: additional_value
    }

    with pytest.raises(KeyError, match=f".*{additional_key}.*"):
        sink.SQLTableSink(config, sql_engine, "test-channel")


@pytest.mark.parametrize("target_column,target_value", [
    ("value_float", "--"),
    ("value_float", {}),
    ("value_int", "--"),
])
def test_sql_sing_invalid_type_conversion(sql_engine, test_table, target_column, target_value):
    """Tests whether the insert fails in case an invalid type is given"""

    config = {
        "table": test_table
    }
    table_sink = sink.SQLTableSink(config, sql_engine, "test-table")

    message = {
        "dp_id": 1,
        "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
        target_column: target_value
    }
    with pytest.raises(exc.MessageFormatError, match=".*[Uu]nable to cast.*"):
        table_sink.insert_messages([message])
