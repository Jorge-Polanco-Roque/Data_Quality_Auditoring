"""Tests para Business Rules Engine."""

import pandas as pd
import pytest

from core.business_rules import BusinessRulesEngine


def test_simple_assertion_pass():
    rules = [{"name": "Price positive", "assertion": "price >= 0", "severity": "HIGH"}]
    df = pd.DataFrame({"price": [10, 20, 30, 0, 5]})
    engine = BusinessRulesEngine(rules)
    results = engine.evaluate(df)
    assert len(results) == 1
    assert results[0].passed


def test_simple_assertion_fail():
    rules = [{"name": "Price positive", "assertion": "price >= 0", "severity": "HIGH"}]
    df = pd.DataFrame({"price": [10, -5, 30, -1, 5]})
    engine = BusinessRulesEngine(rules)
    results = engine.evaluate(df)
    assert len(results) == 1
    assert not results[0].passed
    assert results[0].severity == "HIGH"
    assert results[0].affected_count == 2


def test_conditional_rule():
    rules = [{
        "name": "Refund needs cancellation",
        "condition": "status == 'cancelled'",
        "assertion": "refund > 0",
        "severity": "CRITICAL",
    }]
    df = pd.DataFrame({
        "status": ["active", "cancelled", "cancelled", "active"],
        "refund": [0, 100, 0, 0],
    })
    engine = BusinessRulesEngine(rules)
    results = engine.evaluate(df)
    assert len(results) == 1
    assert not results[0].passed
    assert results[0].affected_count == 1  # row 2 (cancelled but refund=0)


def test_comparison_rule():
    rules = [{"name": "End after start", "assertion": "end > start", "severity": "HIGH"}]
    df = pd.DataFrame({"start": [1, 2, 3], "end": [5, 1, 7]})
    engine = BusinessRulesEngine(rules)
    results = engine.evaluate(df)
    assert len(results) == 1
    assert not results[0].passed
    assert results[0].affected_count == 1  # row 1 (end < start)


def test_empty_dataframe():
    rules = [{"name": "Test rule", "assertion": "x > 0", "severity": "MEDIUM"}]
    df = pd.DataFrame({"x": []})
    engine = BusinessRulesEngine(rules)
    results = engine.evaluate(df)
    assert len(results) == 1
    assert results[0].passed
