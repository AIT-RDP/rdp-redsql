"""
Quickly tests the channel executor
"""
import time

import pytest
import redis
import sqlalchemy as sql

import redsql.channel as channel
import redsql.channel_executor as channel_executor


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
