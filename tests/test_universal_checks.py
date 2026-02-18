"""Tests para checks/universal_checks.py"""

import pandas as pd
import numpy as np
from checks.universal_checks import (
    check_null_rate, check_duplicate_rows, check_whitespace_issues,
    check_constant_column, check_near_constant,
)


def test_null_rate_clean():
    s = pd.Series([1, 2, 3, 4, 5], name="col")
    result = check_null_rate(s, s, {})
    assert result.passed
    assert result.severity == "PASS"


def test_null_rate_high():
    s_raw = pd.Series(["1", "", "null", "nan", "N/A", "missing", "7", "none", "?", "10"], name="col")
    s_typed = pd.to_numeric(s_raw, errors="coerce")
    result = check_null_rate(s_raw, s_typed, {})
    assert not result.passed
    assert result.severity in ("CRITICAL", "HIGH", "MEDIUM")


def test_duplicate_rows():
    df = pd.DataFrame({"a": [1, 1, 2, 3], "b": ["x", "x", "y", "z"]})
    meta = {"_df_raw": df.astype(str)}
    s = df["a"]
    result = check_duplicate_rows(s, s, meta)
    assert not result.passed
    assert result.affected_count == 1


def test_whitespace_issues():
    s_raw = pd.Series(["  hello", "world  ", " both ", "clean"], name="col")
    s_typed = s_raw.str.strip()
    result = check_whitespace_issues(s_raw, s_typed, {})
    assert not result.passed
    assert result.affected_count == 3


def test_constant_column():
    s = pd.Series([42, 42, 42, 42], name="col")
    result = check_constant_column(s, s, {})
    assert not result.passed


def test_constant_column_varied():
    s = pd.Series([1, 2, 3, 4], name="col")
    result = check_constant_column(s, s, {})
    assert result.passed


def test_near_constant():
    s = pd.Series(["A"] * 96 + ["B"] * 4, name="col")
    result = check_near_constant(s, s, {})
    assert not result.passed


def test_near_constant_balanced():
    s = pd.Series(["A"] * 50 + ["B"] * 50, name="col")
    result = check_near_constant(s, s, {})
    assert result.passed
