"""
Tests the SQL-based execution steps
"""

import pytest
import sqlalchemy as sql

import redsql.steps.sql as sql_step


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


def test_cached_sql_query_multiple_open_queries(sql_engine, reference_table):
    """Tests whether it is possible to open multiple queries simultaneously See (#27)"""

    steps = [sql_step.CachedSQLQuery({
        "cache keys": ["dp_id"],
        "query": f"SELECT 'yeah' AS static_text, value_text FROM {reference_table} WHERE dp_id=:dp_id"
    }, channel_name=f"<test-{i}>", step_name=f"<test>-{i}") for i in range(10)]

    for step in steps:
        step.open(sql_engine=sql_engine)

    for step in steps:
        step.close()


def test_cached_sql_query_json_parameter(sql_engine, json_table):
    """Tests the JSON inster capabilities"""
    pass