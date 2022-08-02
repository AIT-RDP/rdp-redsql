"""
Tests the SQL-based execution steps
"""

import pytest
import sqlalchemy as sql

import redsql.steps.sql as sql_step


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
        ])

    yield "reference_table"

    with sql_engine.begin() as con:
        con.execute(sql.text("""
            DROP TABLE reference_table;
        """))


def test_cached_sql_query_single_value(sql_engine, reference_table):
    """Tests the sql query facility using a single value"""

    step = sql_step.CachedSQLQuery({
        "cache keys": ["dp_id"],
        "query": f"SELECT 'yeah' AS static_text, value_text FROM {reference_table} WHERE dp_id=:dp_id"
    }, channel_name="<test>", step_name="<test>")

    step.open(sql_engine=sql_engine)
    messages = list(step.transform_messages([
        {"dp_id": 42, "aux_value": "Supplement"},
        {"dp_id": 42},
        {"dp_id": -1}
    ]))
    step.close()

    assert len(messages) == 3
    assert messages[0] == {
        "dp_id": 42, "aux_value": "Supplement", "static_text": "yeah", "value_text": "One more question is left"
    }
    assert messages[1] == {
        "dp_id": 42, "static_text": "yeah", "value_text": "One more question is left"
    }
    assert messages[2] == {
        "dp_id": -1, "static_text": "yeah", "value_text": "This is the end"
    }


def test_cached_sql_query_multi_value(sql_engine, reference_table):
    """Tests the sql query facility using multiple return values"""

    step = sql_step.CachedSQLQuery({
        "cache keys": ["dp_id"],
        "single value": False,
        "query": f"SELECT 'yeah' AS static_text, value_text FROM {reference_table} WHERE dp_id <= :dp_id ORDER BY dp_id"
    }, channel_name="<test>", step_name="<test>")

    step.open(sql_engine=sql_engine)
    messages = list(step.transform_messages([
        {"dp_id": 42, "aux_value": "Supplement"},
        {"dp_id": -1}
    ]))
    step.close()

    assert len(messages) == 2
    assert messages[0] == {
        "dp_id": 42, "aux_value": "Supplement", "static_text": ["yeah", "yeah"],
        "value_text": ["This is the end", "One more question is left"]
    }
    assert messages[1] == {
        "dp_id": -1, "static_text": ["yeah"], "value_text": ["This is the end"]
    }


def test_cached_sql_query_failure(sql_engine, reference_table):
    """Tests an invalid query response (too many values)"""

    step = sql_step.CachedSQLQuery({
        "cache keys": ["dp_id"],
        "single value": True,
        "query": f"SELECT 'yeah' AS static_text, value_text FROM {reference_table} WHERE dp_id <= :dp_id ORDER BY dp_id"
    }, channel_name="<test>", step_name="<test>")

    step.open(sql_engine=sql_engine)
    with pytest.raises(ValueError):
        list(step.transform_messages([{"dp_id": 42, "aux_value": "Supplement"}]))
    step.close()
