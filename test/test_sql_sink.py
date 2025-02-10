"""
Specifically assesses the SQL sink
"""

import pandas as pd
import pytest

import redsql.sql_sink as sink


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
