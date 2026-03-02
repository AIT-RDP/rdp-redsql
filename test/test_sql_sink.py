"""
Specifically assesses the SQL sink
"""

import pandas as pd
import pytest
import sqlalchemy as sql

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


@pytest.fixture()
def test_views(sql_engine):
    """Creates a simple test view to indirectly insert data"""

    with sql_engine.begin() as con:
        con.execute(sql.text(f"""
            CREATE TABLE test_table_unitemporal (
                    dp_id INTEGER NOT NULL,
                    valid_time TIMESTAMPTZ DEFAULT NULL,
                    value DOUBLE PRECISION,
                    PRIMARY KEY (dp_id, valid_time)
                );
            CREATE OR REPLACE VIEW measurements AS SELECT dp_id, valid_time AS obs_time, value 
                FROM test_table_unitemporal WITH CASCADED CHECK OPTION;
        """))
        con.execute(sql.text(f"""
            CREATE TABLE test_table_bitemporal (
                    dp_id INTEGER NOT NULL,
                    valid_time TIMESTAMPTZ DEFAULT NULL,
                    transaction_time TIMESTAMPTZ DEFAULT NULL,
                    value DOUBLE PRECISION,
                    PRIMARY KEY (dp_id, valid_time, transaction_time)
                );
            CREATE OR REPLACE VIEW forecasts AS SELECT dp_id, valid_time AS obs_time, transaction_time AS fc_time, value 
                FROM test_table_bitemporal WITH CASCADED CHECK OPTION;
        """))

    yield "measurements"

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            DROP VIEW measurements;
            DROP VIEW forecasts;
            DROP TABLE test_table_unitemporal;
            DROP TABLE test_table_bitemporal;
        """))


def test_sql_sink_view_insert(sql_engine, test_views):
    """Tests the data insert on a view"""

    config = {
        "table": "measurements"
    }
    table_sink = sink.SQLTableSink(config, sql_engine, "test-sink")

    message = {
        "dp_id": 1,
        "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
        "value": 32.0
    }

    table_sink.insert_messages([message])

    with sql_engine.connect() as con:
        data = pd.read_sql("""
                SELECT dp_id, valid_time, value FROM test_table_unitemporal ORDER BY dp_id
            """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value": [32.],
    }), check_names=False)


@pytest.mark.parametrize("dup_config_key", [
    "update duplicate values", "update_duplicate_values"
])
def test_sql_sink_view_insert_duplicate_measurements(sql_engine, test_views, dup_config_key):
    """Tests the data insert on a view updating duplicate values"""

    config = {
        "table": "measurements",
        dup_config_key: True  # Test the alias mechanism
    }
    table_sink = sink.SQLTableSink(config, sql_engine, "test-sink")

    messages = [
        {
            "dp_id": 1,
            "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
            "value": 32.0
        },
        {
            "dp_id": 1,
            "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
            "value": 33.0
        }

    ]

    table_sink.insert_messages(messages)

    with sql_engine.connect() as con:
        data = pd.read_sql("""
                SELECT dp_id, valid_time, value FROM test_table_unitemporal ORDER BY dp_id
            """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value": [33.],
    }), check_names=False)


def test_sql_sink_view_insert_incremental_overwrites(sql_engine, test_views):
    """Tests incremental insertions with overwrites - insert, then overwrite multiple times"""

    config = {
        "table": "measurements",
        "update duplicate values": True
    }
    table_sink = sink.SQLTableSink(config, sql_engine, "test-sink")

    # Step 1: Insert initial value
    table_sink.insert_messages([{
        "dp_id": 1,
        "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
        "value": 10.0
    }])

    with sql_engine.connect() as con:
        data = pd.read_sql("SELECT dp_id, valid_time, value FROM test_table_unitemporal ORDER BY dp_id", con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value": [10.],
    }), check_names=False)

    # Step 2: Overwrite with new value
    table_sink.insert_messages([{
        "dp_id": 1,
        "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
        "value": 20.0
    }])

    with sql_engine.connect() as con:
        data = pd.read_sql("SELECT dp_id, valid_time, value FROM test_table_unitemporal ORDER BY dp_id", con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value": [20.],
    }), check_names=False)

    # Step 3: Overwrite again
    table_sink.insert_messages([{
        "dp_id": 1,
        "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
        "value": 30.0
    }])

    with sql_engine.connect() as con:
        data = pd.read_sql("SELECT dp_id, valid_time, value FROM test_table_unitemporal ORDER BY dp_id", con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value": [30.],
    }), check_names=False)

    # Step 4: Insert five duplicates in a batch - only last should win
    table_sink.insert_messages([
        {"dp_id": 1, "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"), "value": 41.0},
        {"dp_id": 1, "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"), "value": 42.0},
        {"dp_id": 1, "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"), "value": 43.0},
        {"dp_id": 1, "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"), "value": 44.0},
        {"dp_id": 1, "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"), "value": 45.0},
    ])

    with sql_engine.connect() as con:
        data = pd.read_sql("SELECT dp_id, valid_time, value FROM test_table_unitemporal ORDER BY dp_id", con)

    # Should have value 45.0 (last one in batch)
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value": [45.],
    }), check_names=False)

    # Step 5: Final overwrite to confirm it still works
    table_sink.insert_messages([{
        "dp_id": 1,
        "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
        "value": 50.0
    }])

    with sql_engine.connect() as con:
        data = pd.read_sql("SELECT dp_id, valid_time, value FROM test_table_unitemporal ORDER BY dp_id", con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "value": [50.],
    }), check_names=False)


def test_sql_sink_view_insert_duplicate_forecasts(sql_engine, test_views):
    """Tests the data insert on a view updating duplicate values"""

    config = {
        "table": "forecasts",
        "update duplicate values": True
    }
    table_sink = sink.SQLTableSink(config, sql_engine, "test-sink")

    messages = [
        {
            "dp_id": 1,
            "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
            "fc_time": pd.to_datetime("2024-12-30T00:00:00Z"),
            "value": 32.0
        },
        {
            "dp_id": 1,
            "obs_time": pd.to_datetime("2024-12-31T00:00:00Z"),
            "fc_time": pd.to_datetime("2024-12-30T00:00:00Z"),
            "value": 33.0
        }

    ]

    table_sink.insert_messages(messages)

    with sql_engine.connect() as con:
        data = pd.read_sql("""
                SELECT dp_id, valid_time, transaction_time, value FROM test_table_bitemporal ORDER BY dp_id
            """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [1],
        "valid_time": pd.to_datetime(["2024-12-31T00:00:00Z"]),
        "transaction_time": pd.to_datetime(["2024-12-30T00:00:00Z"]),
        "value": [33.],
    }), check_names=False)
