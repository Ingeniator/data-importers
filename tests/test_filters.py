"""Tests for dataimporter.filters — parse_filters, _get_nested, _match, apply_filters."""
from __future__ import annotations

import json

import pytest

from dataimporter.filters import FilterRule, _get_nested, _match, apply_filters, parse_filters


# ── parse_filters ─────────────────────────────────────────────────────────────


def test_parse_filters_none_returns_empty():
    assert parse_filters(None) == []


def test_parse_filters_empty_string_returns_empty():
    assert parse_filters("") == []


def test_parse_filters_invalid_json_returns_empty():
    assert parse_filters("{not json}") == []


def test_parse_filters_valid_single_rule():
    raw = json.dumps([{"field": "score", "op": "gt", "value": "5"}])
    rules = parse_filters(raw)
    assert len(rules) == 1
    assert rules[0].field == "score"
    assert rules[0].op == "gt"
    assert rules[0].value == "5"


def test_parse_filters_multiple_rules():
    raw = json.dumps([
        {"field": "a", "op": "eq", "value": "x"},
        {"field": "b", "op": "is_null"},
    ])
    rules = parse_filters(raw)
    assert len(rules) == 2
    assert rules[1].op == "is_null"
    assert rules[1].value is None


def test_parse_filters_malformed_rule_returns_empty():
    # Missing required 'field'
    raw = json.dumps([{"op": "eq", "value": "x"}])
    result = parse_filters(raw)
    assert result == []


# ── _get_nested ───────────────────────────────────────────────────────────────


def test_get_nested_simple_key():
    assert _get_nested({"a": 1}, "a") == 1


def test_get_nested_dot_path():
    assert _get_nested({"a": {"b": {"c": 42}}}, "a.b.c") == 42


def test_get_nested_missing_key_returns_none():
    assert _get_nested({"a": 1}, "b") is None


def test_get_nested_partial_path_returns_none():
    assert _get_nested({"a": {"b": 1}}, "a.b.c") is None


def test_get_nested_non_dict_in_path_returns_none():
    assert _get_nested({"a": "not-a-dict"}, "a.b") is None


def test_get_nested_none_value():
    assert _get_nested({"a": None}, "a") is None


# ── _match ────────────────────────────────────────────────────────────────────


def _rule(field: str, op: str, value: str | None = None) -> FilterRule:
    return FilterRule(field=field, op=op, value=value)


# eq / neq
def test_match_eq_true():
    assert _match({"x": "foo"}, _rule("x", "eq", "foo")) is True


def test_match_eq_false():
    assert _match({"x": "foo"}, _rule("x", "eq", "bar")) is False


def test_match_eq_case_insensitive():
    assert _match({"x": "Foo"}, _rule("x", "eq", "foo")) is True


def test_match_neq_true():
    assert _match({"x": "foo"}, _rule("x", "neq", "bar")) is True


def test_match_neq_false():
    assert _match({"x": "foo"}, _rule("x", "neq", "foo")) is False


# contains / not_contains
def test_match_contains_true():
    assert _match({"x": "hello world"}, _rule("x", "contains", "world")) is True


def test_match_contains_false():
    assert _match({"x": "hello"}, _rule("x", "contains", "world")) is False


def test_match_not_contains_true():
    assert _match({"x": "hello"}, _rule("x", "not_contains", "world")) is True


def test_match_not_contains_false():
    assert _match({"x": "hello world"}, _rule("x", "not_contains", "world")) is False


# starts_with
def test_match_starts_with_true():
    assert _match({"x": "foobar"}, _rule("x", "starts_with", "foo")) is True


def test_match_starts_with_false():
    assert _match({"x": "barfoo"}, _rule("x", "starts_with", "foo")) is False


# is_null / not_null
def test_match_is_null_none():
    assert _match({"x": None}, _rule("x", "is_null")) is True


def test_match_is_null_empty_string():
    assert _match({"x": ""}, _rule("x", "is_null")) is True


def test_match_is_null_missing_key():
    assert _match({}, _rule("x", "is_null")) is True


def test_match_is_null_false():
    assert _match({"x": "something"}, _rule("x", "is_null")) is False


def test_match_not_null_true():
    assert _match({"x": "val"}, _rule("x", "not_null")) is True


def test_match_not_null_false_none():
    assert _match({"x": None}, _rule("x", "not_null")) is False


def test_match_not_null_false_empty():
    assert _match({"x": ""}, _rule("x", "not_null")) is False


# numeric: gt / lt / gte / lte
def test_match_gt_true():
    assert _match({"n": 10}, _rule("n", "gt", "5")) is True


def test_match_gt_false():
    assert _match({"n": 3}, _rule("n", "gt", "5")) is False


def test_match_lt_true():
    assert _match({"n": 2}, _rule("n", "lt", "5")) is True


def test_match_lt_false():
    assert _match({"n": 8}, _rule("n", "lt", "5")) is False


def test_match_gte_equal():
    assert _match({"n": 5}, _rule("n", "gte", "5")) is True


def test_match_gte_false():
    assert _match({"n": 4}, _rule("n", "gte", "5")) is False


def test_match_lte_equal():
    assert _match({"n": 5}, _rule("n", "lte", "5")) is True


def test_match_lte_false():
    assert _match({"n": 6}, _rule("n", "lte", "5")) is False


def test_match_numeric_float():
    assert _match({"n": 3.7}, _rule("n", "gt", "3.5")) is True


def test_match_numeric_non_numeric_field_value_passes_through():
    # Field value can't convert to float → falls through, returns True (unknown)
    assert _match({"n": "abc"}, _rule("n", "gt", "5")) is True


# missing field for non-null ops
def test_match_missing_field_returns_false_for_value_ops():
    # v is None → early return False (before numeric block is reached)
    assert _match({}, _rule("x", "eq", "anything")) is False
    assert _match({}, _rule("x", "contains", "anything")) is False
    assert _match({}, _rule("x", "gt", "0")) is False


# nested fields
def test_match_nested_field_true():
    record = {"meta": {"user": "alice"}}
    assert _match(record, _rule("meta.user", "eq", "alice")) is True


def test_match_nested_field_false():
    record = {"meta": {"user": "alice"}}
    assert _match(record, _rule("meta.user", "eq", "bob")) is False


# ── apply_filters ─────────────────────────────────────────────────────────────

RECORDS = [
    {"name": "alice", "score": 9, "meta": {"active": True}},
    {"name": "bob",   "score": 3, "meta": {"active": False}},
    {"name": "carol", "score": 7, "meta": {"active": True}},
]


def test_apply_filters_no_rules_returns_all():
    result = apply_filters(RECORDS, [])
    assert result == RECORDS


def test_apply_filters_single_rule():
    rules = [FilterRule(field="score", op="gt", value="5")]
    result = apply_filters(RECORDS, rules)
    assert len(result) == 2
    assert all(r["score"] > 5 for r in result)


def test_apply_filters_multiple_rules_and_semantics():
    rules = [
        FilterRule(field="score", op="gt", value="5"),
        FilterRule(field="name", op="contains", value="a"),
    ]
    result = apply_filters(RECORDS, rules)
    # alice (score=9) and carol (score=7) both contain 'a'
    assert len(result) == 2
    assert {r["name"] for r in result} == {"alice", "carol"}


def test_apply_filters_no_matches():
    rules = [FilterRule(field="score", op="gt", value="100")]
    assert apply_filters(RECORDS, rules) == []


def test_apply_filters_nested_field():
    rules = [FilterRule(field="meta.active", op="eq", value="true")]
    result = apply_filters(RECORDS, rules)
    assert len(result) == 2
    assert all(r["meta"]["active"] for r in result)


def test_apply_filters_empty_records():
    rules = [FilterRule(field="score", op="gt", value="5")]
    assert apply_filters([], rules) == []
