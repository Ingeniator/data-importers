"""Server-side field filtering applied after backend queries return results."""

from __future__ import annotations

import json

from pydantic import BaseModel


class FilterRule(BaseModel):
    field: str
    # Supported ops: eq, neq, contains, not_contains, starts_with,
    #                gt, lt, gte, lte, is_null, not_null
    op: str
    value: str | None = None


def _get_nested(record: dict, path: str):
    """Traverse a dot-notation path through a nested dict. Returns None if missing."""
    curr: object = record
    for key in path.split("."):
        if not isinstance(curr, dict):
            return None
        curr = curr.get(key)  # type: ignore[union-attr]
    return curr


def _match(record: dict, rule: FilterRule) -> bool:
    v = _get_nested(record, rule.field)

    if rule.op == "is_null":
        return v is None or v == ""
    if rule.op == "not_null":
        return v is not None and v != ""
    if v is None:
        return False

    sv = str(v).lower()
    fv = (rule.value or "").lower()

    if rule.op == "contains":       return fv in sv
    if rule.op == "not_contains":   return fv not in sv
    if rule.op == "eq":             return sv == fv
    if rule.op == "neq":            return sv != fv
    if rule.op == "starts_with":    return sv.startswith(fv)

    # Numeric comparisons — fall back gracefully if value isn't a number
    try:
        nv  = float(str(v))
        nfv = float(rule.value or "0")
        if rule.op == "gt":  return nv > nfv
        if rule.op == "lt":  return nv < nfv
        if rule.op == "gte": return nv >= nfv
        if rule.op == "lte": return nv <= nfv
    except (TypeError, ValueError):
        pass

    return True  # unknown op: pass through


def apply_filters(records: list[dict], rules: list[FilterRule]) -> list[dict]:
    for rule in rules:
        records = [r for r in records if _match(r, rule)]
    return records


def parse_filters(filters_json: str | None) -> list[FilterRule]:
    """Parse a JSON-encoded array of filter rules. Returns [] on any error."""
    if not filters_json:
        return []
    try:
        data = json.loads(filters_json)
        return [FilterRule(**item) for item in data]
    except Exception:
        return []
