"""
Provides common test fixtures for all test cases
"""

import os
import logging

import prometheus_client as prom
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


@pytest.fixture()
def performance_sql_engine() -> sql.engine.Engine:
    """Returns a connected engine referencing the test DB instance using a high-performance configuration"""

    engine_url = os.environ["REDSQL_DB_URL"]  # e.g. postgresql://postgres:test@localhost/testing_db
    engine = sql.create_engine(engine_url)
    engine.connect()

    return engine


@pytest.fixture()
def reference_table(sql_engine: sql.engine.Engine) -> str:
    """temporary creates a testing table and returns its name"""

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            CREATE TABLE reference_table (
                dp_id INTEGER NOT NULL,
                value_text TEXT 
            );
        """))
        con.execute(sql.text("INSERT INTO reference_table(dp_id, value_text) VALUES (:dp_id, :value_text)"), [
            dict(dp_id=-1, value_text="This is the end"),
            dict(dp_id=42, value_text="One more question is left"),
            dict(dp_id=666, value_text="My name is legion"),
            *[dict(dp_id=100 + i, value_text=f"The {i}th beyond 100") for i in range(100)]
        ])

    yield "reference_table"

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            DROP TABLE reference_table;
        """))


@pytest.fixture()
def json_table(sql_engine: sql.engine.Engine) -> str:
    """Creates and pre-fills a simple tables that hosts some json objects"""

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            CREATE TABLE json_table (
                meta_id INTEGER NOT NULL,
                first_object JSON DEFAULT '{}'::JSON,
                second_object JSONB DEFAULT '{}'::JSONB
            );
        """))
        stmt = sql.text("INSERT INTO json_table(meta_id, first_object, second_object) VALUES (:id, :first, :second)")
        stmt = stmt.bindparams(sql.bindparam('first', type_=sql.JSON), sql.bindparam('second', type_=sql.JSON))
        con.execute(stmt,
            [
                dict(
                    id=0,
                    first={"location": "the first in line", "nested": {"yes": "we can"}},
                    second={"query_support": True, "nested": {"really": True}}
                ),
                dict(id=1, first={"location": "the second in line", "nested": {"no": "not this time"}}, second={}),
            ]
        )

    yield "json_table"

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            DROP TABLE json_table;
        """))


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
