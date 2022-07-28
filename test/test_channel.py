"""
Specifically tests the channel functionality that reads messages from Redis and relays them to the database
"""
import datetime

import pandas as pd
import pytest
import redis
import sqlalchemy as sql

import redsql.channel as channel


@pytest.fixture()
def reduced_channel_config() -> dict:
    """Exposes a minimal configuration to test the basic implementation"""

    return {
        "trigger": {
            "stream id": "test.stream",
        },
        "data sink": {
            "table": "test_table",
            "columns": {
                "value_int": "my int",
                "value_float": "my float"
            }
        }
    }


@pytest.fixture()
def test_table(sql_engine: sql.engine.Engine) -> str:
    """temporary creates a testing table and returns its name"""

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            CREATE TABLE test_table (
                dp_id INTEGER NOT NULL,
                obs_time TIMESTAMPTZ DEFAULT NULL,
                value_int INTEGER NOT NULL DEFAULT 42,
                value_float DOUBLE PRECISION,
                value_text TEXT DEFAULT 'Nothing to add' 
            );
        """))

    yield "test_table"

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            DROP TABLE test_table;
        """))


def read_test_table(sql_engine: sql.engine.Engine) -> pd.DataFrame:
    """
    Reads the test table into a DataFrame and returns it
    :param sql_engine: The SQL engine to read the data from
    :return: The entire test table content
    """

    with sql_engine.begin() as con:
        ret = pd.read_sql(sql.text("""
            SELECT dp_id, obs_time, value_int, value_float, value_text FROM test_table;
        """), con, index_col="dp_id")
    ret = ret.sort_index()
    ret.index.name = None  # Mare writing reference tables easier
    return ret


def test_channel_pass_through(reduced_channel_config, redis_pool, sql_engine, test_table):
    """Tests a channel without any processing steps directly writing to an SQL table"""

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd("test.stream", {
        "my int": 666,
        "my float": 0.2,
        "dp_id": -1
    })
    redis_client.xadd("test.stream", {
        "value_text": "Let the HammerFall! \U0001F918",
        "my float": 0.9,
        "dp_id": 22
    })

    chn = channel.Channel(reduced_channel_config, "test channel")

    chn.open(redis_pool, sql_engine)
    for _ in range(2):
        chn.execute_channel_once()
    chn.close()

    table_content = read_test_table(sql_engine)

    assert table_content is not None
    pd.testing.assert_frame_equal(table_content, pd.DataFrame({
        "obs_time": [None, None],
        "value_int": [666, 42],
        "value_float": [0.2, 0.9],
        "value_text": ["Nothing to add", "Let the HammerFall! \U0001F918"]
    }, index=[-1, 22]))


# TODO: Test invalid data types
# TODO: Test execute_channel_once() without a message
# TODO: Test data conversion