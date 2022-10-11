"""
Quickly tests the channel executor
"""
import time

import prometheus_client as prom
import pytest
import redis
import sqlalchemy as sql

import redsql.channel as channel
import redsql.channel_executor as channel_executor
import test_channel


class MockupChannel(channel.Channel):
    """A mockup channel to test the channel execution"""

    def __init__(self):
        """Initializes the mockup"""
        # don't call super(), to avoid the channel overhead
        self.exec_invocations = 0
        self.open_invocations = 0
        self.close_invocations = 0

    def open(self, redis_pool: redis.ConnectionPool, sql_engine: sql.engine.Engine):
        """Just counts the number of executions"""
        # don't call super(), to avoid the channel overhead
        self.open_invocations += 1

    def execute_channel_once(self):
        """Just counts the number of executions"""
        time.sleep(0.2)
        self.exec_invocations += 1

    def close(self):
        """Just counts the number of executions"""
        # don't call super(), to avoid the channel overhead
        self.close_invocations += 1


def test_thread_executor_lifecycle(redis_pool, sql_engine):
    """Tests the basic lifecycle of the thread executor"""

    mockup_channel = MockupChannel()
    executor = channel_executor.ThreadChannelExecutor({}, redis_pool, sql_engine, "test-channel", mockup_channel)

    with pytest.raises(AssertionError):
        executor.join()

    executor.start()

    with pytest.raises(AttributeError):
        print(executor.channel)

    executor.stop()

    with pytest.raises(AssertionError):
        executor.stop()

    executor.join()


def test_thread_executor_channel_calls(redis_pool, sql_engine):
    """Tests whether the channel functions are correctly called"""

    mockup_channel = MockupChannel()
    executor = channel_executor.ThreadChannelExecutor({}, redis_pool, sql_engine, "test-channel", mockup_channel)

    assert executor.channel == mockup_channel
    assert mockup_channel.exec_invocations == 0

    executor.start()
    time.sleep(0.41)
    executor.stop()
    executor.join()

    assert 1 <= mockup_channel.exec_invocations <= 3
    assert mockup_channel.open_invocations == 1
    assert mockup_channel.close_invocations == 1


def test_bulk_channel_operation(redis_pool, performance_sql_engine, reference_table, test_table):
    """Tests the end-to-end operation using several concurrent channels"""

    num_channels = 30
    num_messages = 5

    redis_client = redis.Redis(connection_pool=redis_pool)

    config = {}
    for channel_nr in range(num_channels):

        # Assemble the individual channel configuration
        config[f"Chn.{channel_nr}"] = {
            "trigger": {"stream id": f"test-stream.{channel_nr}"},
            "encoding": {
                "_default": "JSON",
                "observation_time": "JSONDatetimeString"
            },
            "steps": [
                {
                    "type": "CachedSQLQuery",
                    "cache keys": ["dp_id"],
                    "query": f"SELECT value_text FROM {reference_table} WHERE dp_id=:dp_id"
                }
            ],
            "data sink": {
                "table": test_table,
                "columns": {
                    "dp_id": "dp_id",
                    "value_text": "value_text",
                    "value_float": "value_float",
                    "value_int": "value_int"
                }
            }
        }

        # Add some test messages
        for message_nr in range(num_messages):
            redis_client.xadd(f"test-stream.{channel_nr}", fields={
                "dp_id": f"{100 + (message_nr % 100)}",
                "value_float": f"{message_nr * num_channels + channel_nr}",
                "value_int": f"{channel_nr}"
            })

    supervisor = channel_executor.ChannelSupervisor(config, redis_pool, performance_sql_engine)
    supervisor.start()
    try:
        while test_channel.read_test_table(performance_sql_engine,
                                           test_table).index.size != num_channels * num_messages:
            time.sleep(1)
            status = supervisor.heartbeat()
            assert status == {f"Chn.{i}": "ok" for i in range(num_channels)}
    finally:
        supervisor.stop()


class ErrMockupChannel(channel.Channel):
    """Raises an error, if requested"""

    def __init__(self, *args, **kwargs):
        self.raise_exc = False

    def open(self, *args, **kwargs):
        pass

    def execute_channel_once(self):
        """Just counts the number of executions"""

        time.sleep(0.1)
        if self.raise_exc:
            raise ValueError("Time to say goodbye")

    def close(self):
        pass


def test_channel_supervisor(redis_pool, sql_engine):
    """Tests the restart-capabilities of the channel supervisor"""

    err_mockup = ErrMockupChannel()
    supervisor = channel_executor.ChannelSupervisor({
        "error prone channel": {}
    }, redis_pool, sql_engine, ext_channels={
        "error prone channel": err_mockup
    })

    supervisor.start()
    time.sleep(0.1)

    status = supervisor.heartbeat()
    assert status["error prone channel"] == "ok"

    err_mockup.raise_exc = True
    time.sleep(0.15)

    status = supervisor.heartbeat()
    assert status["error prone channel"] == "restarted"

    supervisor.stop()
