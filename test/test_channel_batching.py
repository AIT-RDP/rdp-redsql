"""
Tests the performance features: no_ack and batch_interval
"""
import time
import pandas as pd
import pytest
import redis
import sqlalchemy as sql

import redsql.channel as channel


@pytest.fixture()
def test_table_indexed(sql_engine: sql.engine.Engine) -> str:
    """Creates an indexed testing table and returns its name"""

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            CREATE TABLE test_table_indexed (
                dp_id INTEGER NOT NULL,
                obs_time TIMESTAMPTZ DEFAULT NULL,
                value_int INTEGER NOT NULL DEFAULT 42,
                value_float DOUBLE PRECISION,
                value_text TEXT DEFAULT 'Nothing to add',
                PRIMARY KEY (dp_id)
            );
        """))

    yield "test_table_indexed"

    with sql_engine.begin() as con:
        con.execute(sql.text("DROP TABLE test_table_indexed;"))


@pytest.fixture()
def redis_test_stream(redis_pool) -> str:
    """Creates a redis stream and returns it afterwards"""

    stream_name = "test.stream.perf"
    redis_client = redis.Redis(connection_pool=redis_pool)

    redis_client.xgroup_create(stream_name, "group.test.fixture", "0-0", mkstream=True)
    yield stream_name
    redis_client.delete(stream_name)


def read_test_table(sql_engine: sql.engine.Engine, table_name="test_table") -> pd.DataFrame:
    """Reads the test table into a DataFrame and returns it"""

    with sql_engine.connect() as con:
        ret = pd.read_sql(f"SELECT dp_id, value_int, value_float, value_text FROM {table_name};",
                          con.connection, index_col="dp_id")
    ret = ret.sort_index()
    ret.index.name = None
    return ret


def test_no_ack_feature_explicit(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that no_ack can be explicitly enabled in trigger config"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
            "no_ack": True  # Explicitly enable no_ack
        },
        "data sink": {
            "table": test_table
        }
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(redis_test_stream, {"dp_id": 1, "value_float": 1.1})
    redis_client.xadd(redis_test_stream, {"dp_id": 2, "value_float": 2.2})

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)
    chn.execute_channel_once()
    chn.execute_channel_once()
    chn.close()

    # Verify data was inserted
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 2
    assert table_content.loc[1, "value_float"] == 1.1
    assert table_content.loc[2, "value_float"] == 2.2


def test_no_ack_default_false(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that no_ack defaults to False when not specified"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
            # no_ack not specified - should default to False
        },
        "data sink": {
            "table": test_table
        }
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(redis_test_stream, {"dp_id": 1, "value_float": 1.1})

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    # Verify default is False (ACK enabled)
    assert chn._data_source._no_ack == False

    chn.execute_channel_once()
    chn.close()

    # Verify data was inserted
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 1


def test_batch_interval_immediate_insert(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that without batch_interval, messages are inserted immediately"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
        },
        "data sink": {
            "table": test_table,
            # batch_interval not specified - immediate insert
        }
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(redis_test_stream, {"dp_id": 1, "value_float": 1.1})
    redis_client.xadd(redis_test_stream, {"dp_id": 2, "value_float": 2.2})

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    # Each call should insert immediately
    chn.execute_channel_once()
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 1  # First message inserted

    chn.execute_channel_once()
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 2  # Second message inserted

    chn.close()


def test_batch_interval_batching(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that batch_interval accumulates messages before flushing"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
        },
        "data sink": {
            "table": test_table,
            "batch_interval": 0.5  # 500ms batching
        }
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(redis_test_stream, {"dp_id": 1, "value_float": 1.1})
    redis_client.xadd(redis_test_stream, {"dp_id": 2, "value_float": 2.2})
    redis_client.xadd(redis_test_stream, {"dp_id": 3, "value_float": 3.3})

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    # Process first message - should buffer, not insert yet
    chn.execute_channel_once()
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 0  # Buffered, not inserted

    # Process second message quickly - still buffered
    chn.execute_channel_once()
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 0  # Still buffered

    # Wait for batch interval to pass
    time.sleep(0.6)

    # Process third message - should flush the batch
    chn.execute_channel_once()
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 3  # All 3 messages flushed

    chn.close()


def test_batch_interval_auto_enables_no_ack(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that batch_interval automatically enables no_ack"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
            # no_ack not specified
        },
        "data sink": {
            "table": test_table,
            "batch_interval": 1.0  # Should auto-enable no_ack
        }
    }

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    # Verify no_ack was auto-enabled
    assert chn._data_source._no_ack == True

    chn.close()


def test_batch_interval_respects_explicit_no_ack_false(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that explicit no_ack=false is respected even with batching"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
            "no_ack": False  # Explicitly disable
        },
        "data sink": {
            "table": test_table,
            "batch_interval": 1.0
        }
    }

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    # Verify explicit config is respected
    assert chn._data_source._no_ack == False

    chn.close()


def test_batch_interval_with_update_duplicates(redis_pool, sql_engine, test_table_indexed, redis_test_stream):
    """Tests that batch_interval works with update_duplicate_values=True"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
        },
        "data sink": {
            "table": test_table_indexed,
            "batch_interval": 0.5,
            "update duplicate values": True  # Should use executemany, not COPY
        }
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(redis_test_stream, {"dp_id": 1, "value_float": 1.1})
    redis_client.xadd(redis_test_stream, {"dp_id": 1, "value_float": 2.2})  # Duplicate - should update

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    # Process both messages
    chn.execute_channel_once()
    chn.execute_channel_once()

    # Wait for batch to flush
    time.sleep(0.6)

    # Trigger flush by closing
    chn.close()

    # Verify upsert worked - should have 1 row with updated value
    table_content = read_test_table(sql_engine, test_table_indexed)
    assert len(table_content) == 1
    assert table_content.loc[1, "value_float"] == 2.2  # Updated to second value


def test_batch_interval_without_update_duplicates(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that batch_interval works without update_duplicate_values (allows COPY)"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
        },
        "data sink": {
            "table": test_table,
            "batch_interval": 0.5,
            "update duplicate values": False  # Should allow COPY
        }
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    for i in range(10):
        redis_client.xadd(redis_test_stream, {"dp_id": i, "value_float": float(i)})

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    # Process all messages quickly
    for _ in range(10):
        chn.execute_channel_once()

    # Wait for batch to flush
    time.sleep(0.6)

    # Trigger another execute to flush
    chn.execute_channel_once()

    chn.close()

    # Verify all messages were inserted
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 10


def test_batch_interval_flush_on_close(redis_pool, sql_engine, test_table, redis_test_stream):
    """Tests that remaining buffered messages are flushed on close"""

    config = {
        "trigger": {
            "stream id": redis_test_stream,
        },
        "data sink": {
            "table": test_table,
            "batch_interval": 10.0  # Very long interval
        }
    }

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(redis_test_stream, {"dp_id": 1, "value_float": 1.1})

    chn = channel.Channel(config, "test channel")
    chn.open(redis_pool, sql_engine)

    chn.execute_channel_once()

    # Message should be buffered, not inserted yet
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 0

    # Close should flush the buffer
    chn.close()

    # Now message should be inserted
    table_content = read_test_table(sql_engine)
    assert len(table_content) == 1
