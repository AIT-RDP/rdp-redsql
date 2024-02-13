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
    """Tests the JSON insert capabilities"""

    step = sql_step.CachedSQLQuery({
        "cache keys": ["meta_id"],
        "parameter types": {"first_object": "JsOn", "second_object": "JSONB"},
        "query": f"""
            INSERT INTO {json_table}(meta_id, first_object, second_object) 
            VALUES (:meta_id, :first_object, :second_object)
            RETURNING meta_id AS meta
        """
    }, channel_name="<test>", step_name="<test>")

    step.open(sql_engine=sql_engine)
    messages = list(step.transform_messages([
        {"meta_id": 42, "first_object": {"location": "Here", "nested": {"yes": "It's nested"}}, "second_object": {}},
        {"meta_id": 43, "first_object": {}, "second_object": {"location": "Here", "nested": {"yes": "It's nested"}}},
    ]))
    step.close()

    # Check the extended message
    assert len(messages) == 2
    assert messages[0]["meta"] == 42
    assert messages[1]["meta"] == 43

    # Check the database content
    for row in sql_engine.execute(sql.text(f"SELECT first_object, second_object FROM {json_table} WHERE meta_id = 42")):
        assert row[0] == {"location": "Here", "nested": {"yes": "It's nested"}}
        assert row[1] == {}


def test_cached_sql_query_unknown_parameter_type(sql_engine, json_table):
    """Simply tests if the step raises a useful error message in case the parameter type information is invalid."""

    with pytest.raises(KeyError) as ex_handler:
        sql_step.CachedSQLQuery({
            "cache keys": ["meta_id"],
            "parameter types": {"first_object": "JSON", "second_object": "SomeEpicType"},
            "query": f"""
                INSERT INTO {json_table}(meta_id, first_object, second_object) 
                VALUES (:meta_id, :first_object, :second_object)
                RETURNING meta_id AS meta
            """
        }, channel_name="<test>", step_name="<test>")

    assert "SomeEpicType" in str(ex_handler.value)
