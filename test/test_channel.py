"""
Specifically tests the channel functionality that reads messages from Redis and relays them to the database
"""
import datetime
from typing import Dict, Any

import pandas as pd
import pytest
import redis
import sqlalchemy as sql

import redsql.channel as channel
import redsql.exc as exc
import redsql.steps.abc.step as abstract_step


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


@pytest.fixture()
def redis_test_stream(redis_pool) -> str:
    """Creates a redis stream and returns it afterwards"""

    stream_name = "test.stream"
    redis_client = redis.Redis(connection_pool=redis_pool)

    redis_client.xgroup_create(stream_name, "group.test.fixture", "0-0", mkstream=True)
    yield stream_name
    redis_client.delete(stream_name)


def read_test_table(sql_engine: sql.engine.Engine) -> pd.DataFrame:
    """
    Reads the test table into a DataFrame and returns it
    :param sql_engine: The SQL engine to read the data from
    :return: The entire test table content
    """

    with sql_engine.connect() as con:
        ret = pd.read_sql(sql.text("""
            SELECT dp_id, obs_time, value_int, value_float, value_text FROM test_table;
        """), con, index_col="dp_id")
    ret = ret.sort_index()
    ret.index.name = None  # Mare writing reference tables easier
    return ret


def test_channel_pass_through(reduced_channel_config, redis_pool, sql_engine, test_table, redis_test_stream):
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


def test_channel_invalid_sql_type(reduced_channel_config, redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests the channel with an invalid SQL type"""

    redis_client = redis.Redis(connection_pool=redis_pool)
    # Add invalid messages:
    redis_client.xadd("test.stream", {
        "my int": "definitely-no-number",  # ERROR: should be an integer
        "my float": 0.2,
        "dp_id": -1
    })
    redis_client.xadd("test.stream", {
        "my int": 666,
        "my float": "roughly-pi",  # ERROR: should be a floating point number
        "dp_id": -1
    })
    redis_client.xadd("test.stream", {
        "my float": 0.9
        # ERROR: no mandatory dp_id field
    })

    # Add a correct message to check whether the channel can still correctly handle new messages:
    redis_client.xadd("test.stream", {
        "value_text": "Let the HammerFall! \U0001F918",
        "my float": 0.9,
        "dp_id": 22
    })

    # Operate the channel and check whether the correct exceptions are raised
    chn = channel.Channel(reduced_channel_config, "test channel")
    chn.open(redis_pool, sql_engine)

    with pytest.raises(exc.MessageFormatError, match=r"definitely\-no\-number"):
        chn.execute_channel_once()
    with pytest.raises(exc.MessageFormatError, match=r"roughly\-pi"):
        chn.execute_channel_once()
    with pytest.raises(exc.MessageFormatError, match=r"dp_id"):
        chn.execute_channel_once()
    chn.execute_channel_once()

    chn.close()

    table_content = read_test_table(sql_engine)

    assert table_content is not None
    pd.testing.assert_frame_equal(table_content, pd.DataFrame({
        "obs_time": [None],
        "value_int": [42],
        "value_float": [0.9],
        "value_text": ["Let the HammerFall! \U0001F918"]
    }, index=[22]))


def test_channel_encoding(reduced_channel_config, redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests whether the encoding step is correctly set"""

    reduced_channel_config["encoding"] = {
        "obs_time": "JSONDatetimeString"
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd("test.stream", {
        "obs_time": "\"2022-08-01T12:00:00Z\"",
        "my int": 666,
        "my float": 0.2,
        "dp_id": -1
    })

    chn = channel.Channel(reduced_channel_config, "test channel")
    chn.open(redis_pool, sql_engine)
    chn.execute_channel_once()
    chn.close()

    table_content = read_test_table(sql_engine)

    assert table_content is not None
    pd.testing.assert_frame_equal(table_content, pd.DataFrame({
        "obs_time": [datetime.datetime(2022, 8, 1, 12, 0, tzinfo=datetime.timezone.utc)],
        "value_int": [666],
        "value_float": [0.2],
        "value_text": ["Nothing to add"]
    }, index=[-1]))


def test_channel_without_messages(reduced_channel_config, redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests the channel without receiving a message"""

    chn = channel.Channel(reduced_channel_config, "test channel")
    chn.open(redis_pool, sql_engine)

    chn.execute_channel_once()  # No message today

    chn.close()


class MockupStep(abstract_step.AbstractOneToOneStep):
    """Mockup to test the instantiation of transformation steps"""

    def __init__(self, channel_name, step_name, config, **kwargs):
        super(MockupStep, self).__init__(**kwargs)

        assert len(kwargs) == 0
        self.channel_name = channel_name
        self.step_name = step_name
        self.config = config

        self.state = "no-open"

    def open(self, **kwargs):
        assert self.state == "no-open"
        self.state = "open"

    def transform_single_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        assert self.state == "open"

        message = message.copy()
        old_txt = message.get("value_text", "")
        message["value_text"] = f"step-{self.channel_name}:{self.step_name}:{self.config.get('msg', '')}:{old_txt}"
        return message

    def close(self):
        assert self.state == "open"
        self.state = "closed"

    def __del__(self):
        assert self.state in ["closed", "no-open"]


def test_channel_transformation(reduced_channel_config, redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests two simple mockup transformation steps"""

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd("test.stream", {
        "my int": 666,
        "my float": 0.2,
        "dp_id": -1
    })

    reduced_channel_config["steps"] = [
        {"type": "test_channel.MockupStep", "msg": "first"},
        {"type": "test_channel.MockupStep", "msg": "second"},
    ]
    chn = channel.Channel(reduced_channel_config, "test channel")

    chn.open(redis_pool, sql_engine)
    chn.execute_channel_once()
    chn.close()

    table_content = read_test_table(sql_engine)

    assert table_content is not None
    pd.testing.assert_frame_equal(table_content, pd.DataFrame({
        "obs_time": [None],
        "value_int": [666],
        "value_float": [0.2],
        "value_text": ["step-test channel:step-2:second:step-test channel:step-1:first:"]
    }, index=[-1]))


def test_channel_step_instantiation(reduced_channel_config, redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests the step instantiation that dynamically loads default processing steps"""

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd("test.stream", {
        "dp_id": -1,
        "source a": 41.99,
        "source b": 42.01
    })

    reduced_channel_config["steps"] = [{
        "type": "SplitByKey",
        "always include": ["dp_id"],
        "destination key": "my float",
        "source output key": "value_text"
    }]
    chn = channel.Channel(reduced_channel_config, "test channel")

    chn.open(redis_pool, sql_engine)
    chn.execute_channel_once()
    chn.close()

    table_content = read_test_table(sql_engine)

    assert table_content is not None
    table_content = table_content.sort_values("value_text", axis="index")
    pd.testing.assert_frame_equal(table_content, pd.DataFrame({
        "obs_time": [None, None],
        "value_int": [42, 42],
        "value_float": [41.99, 42.01],
        "value_text": ["source a", "source b"]
    }, index=[-1, -1]))
