"""Tests para checks/null_pattern_checks.py"""

import pandas as pd
import numpy as np
from checks.null_pattern_checks import run_null_pattern_checks


def test_correlated_nulls():
    np.random.seed(42)
    n = 200
    df = pd.DataFrame({"a": np.random.normal(0, 1, n), "b": np.random.normal(0, 1, n), "c": range(n)})
    # Same rows null in a and b
    mask = np.random.random(n) < 0.2
    df.loc[mask, "a"] = np.nan
    df.loc[mask, "b"] = np.nan
    results = run_null_pattern_checks(df, df.astype(str))
    null_corr = [r for r in results if r.check_id == "NULL_CORRELATION"]
    assert len(null_corr) >= 1


def test_no_nulls():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    results = run_null_pattern_checks(df, df.astype(str))
    null_corr = [r for r in results if r.check_id == "NULL_CORRELATION"]
    assert len(null_corr) == 0


def test_row_patterns():
    df = pd.DataFrame({
        "a": [1, np.nan, np.nan, 4, np.nan],
        "b": [1, np.nan, np.nan, 4, np.nan],
        "c": [1, np.nan, np.nan, 4, np.nan],
    })
    results = run_null_pattern_checks(df, df.astype(str))
    row_checks = [r for r in results if r.check_id == "NULL_ROW_PATTERN"]
    assert len(row_checks) >= 1
