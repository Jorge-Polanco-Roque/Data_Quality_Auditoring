"""Tests para checks/categorical_checks.py"""

import pandas as pd
import numpy as np
from checks.categorical_checks import (
    check_rare_categories, check_case_inconsistency,
    check_encoding_anomaly, check_class_imbalance,
    check_typo_candidates, check_cardinality_change,
)


def _meta():
    return {}


def test_rare_categories():
    vals = ["A"] * 490 + ["B"] * 500 + ["C"] * 1 + ["D"] * 1 + ["E"] * 8
    s = pd.Series(vals, name="col")
    result = check_rare_categories(s, s, _meta())
    assert result.check_id == "RARE_CATEGORIES"


def test_no_rare_categories():
    s = pd.Series(["A"] * 50 + ["B"] * 50, name="col")
    result = check_rare_categories(s, s, _meta())
    assert result.passed


def test_case_inconsistency():
    s = pd.Series(["Cat", "cat", "CAT", "Dog", "dog"], name="col")
    result = check_case_inconsistency(s, s, _meta())
    assert not result.passed
    assert result.value >= 2  # 'cat' and 'dog' groups


def test_no_case_inconsistency():
    s = pd.Series(["cat", "dog", "bird"], name="col")
    result = check_case_inconsistency(s, s, _meta())
    assert result.passed


def test_encoding_anomaly_clean():
    s = pd.Series(["hello", "world", "café"], name="col")
    result = check_encoding_anomaly(s, s, _meta())
    assert result.passed


def test_class_imbalance():
    s = pd.Series(["A"] * 96 + ["B"] * 4, name="col")
    result = check_class_imbalance(s, s, _meta())
    assert not result.passed


def test_class_balanced():
    s = pd.Series(["A"] * 50 + ["B"] * 50, name="col")
    result = check_class_imbalance(s, s, _meta())
    assert result.passed


def test_typo_candidates():
    s = pd.Series(["California", "Calfornia", "Texas", "Texsa"] * 10, name="col")
    result = check_typo_candidates(s, s, _meta())
    assert result.check_id == "TYPO_CANDIDATES"
    # Should detect California/Calfornia and Texas/Texsa as typo pairs


def test_cardinality_change():
    s = pd.Series(["A", "B", "C", "D", "E"], name="col")
    result = check_cardinality_change(s, s, _meta())
    assert result.check_id == "CARDINALITY_CHANGE"
    assert result.value == 5.0
