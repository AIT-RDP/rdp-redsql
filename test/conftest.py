"""
Provides common test fixtures for all test cases
"""

import os
import logging

import pytest
import redis
import sqlalchemy as sql

logger = logging.getLogger(__name__)


@pytest.fixture()
def redis_pool() -> redis.ConnectionPool:
    """Opens a Redis pool and tests the connection"""

    host = os.environ.get("REDSQL_REDIS_HOST", "localhost")
    port = os.environ.get("REDSQL_REDIS_PORT", "6379")
    db = os.environ.get("REDSQL_REDIS_DB", "0")

    logger.debug(f"Initialize redis pool connecting to host={host}, port={port}, db={db}")

    pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
    client = redis.Redis(connection_pool=pool)
    client.ping()

    return pool


@pytest.fixture()
def sql_engine() -> sql.engine.Engine:
    """Returns a connected engine referencing the test DB instance"""

    engine_url = os.environ["REDSQL_DB_URL"]  # e.g. postgresql://postgres:test@localhost/testing_db
    engine = sql.create_engine(engine_url, pool_size=2, max_overflow=2, pool_timeout=2)
    engine.connect()

    return engine
