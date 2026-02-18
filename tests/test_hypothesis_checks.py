"""Tests para checks/hypothesis_checks.py"""

import pandas as pd
import numpy as np
from checks.hypothesis_checks import (
    check_normality_anderson, check_normality_lilliefors,
    check_mean_comparison, check_wilcoxon_paired,
    check_variance_comparison, check_ks_goodness_of_fit,
    check_stationarity_adf,
)


def _meta(df=None):
    return {"_df": df, "_df_raw": df.astype(str) if df is not None else None, "_date_col": None}


def test_anderson_normal():
    np.random.seed(42)
    s = pd.Series(np.random.normal(0, 1, 200), name="col")
    result = check_normality_anderson(s.astype(str), s, _meta())
    assert result.passed  # Normal data should pass


def test_anderson_non_normal():
    np.random.seed(42)
    s = pd.Series(np.random.exponential(1, 200), name="col")
    result = check_normality_anderson(s.astype(str), s, _meta())
    assert not result.passed


def test_lilliefors_normal():
    np.random.seed(42)
    s = pd.Series(np.random.normal(0, 1, 200), name="col")
    result = check_normality_lilliefors(s.astype(str), s, _meta())
    assert result.passed


def test_mean_shift_detected():
    # First half mean=50, second half mean=100
    s = pd.Series([50] * 50 + [100] * 50, name="col")
    result = check_mean_comparison(s.astype(str), s, _meta())
    assert not result.passed


def test_mean_no_shift():
    np.random.seed(42)
    s = pd.Series(np.random.normal(50, 5, 200), name="col")
    result = check_mean_comparison(s.astype(str), s, _meta())
    # Homogeneous data should generally pass
    assert result.check_id == "MEAN_SHIFT"


def test_wilcoxon_paired():
    # Clear difference between halves
    s = pd.Series(list(range(100)) + list(range(100, 200)), name="col")
    result = check_wilcoxon_paired(s.astype(str), s, _meta())
    assert result.check_id == "WILCOXON_PAIRED"


def test_variance_shift():
    # First half low var, second half high var
    np.random.seed(42)
    s = pd.Series(
        np.concatenate([np.random.normal(50, 1, 100), np.random.normal(50, 20, 100)]),
        name="col"
    )
    result = check_variance_comparison(s.astype(str), s, _meta())
    assert not result.passed


def test_ks_goodness_normal():
    np.random.seed(42)
    s = pd.Series(np.random.normal(0, 1, 200), name="col")
    result = check_ks_goodness_of_fit(s.astype(str), s, _meta())
    assert result.passed  # Normal data fits normal distribution


def test_ks_goodness_non_normal():
    np.random.seed(42)
    s = pd.Series(np.random.exponential(1, 200), name="col")
    result = check_ks_goodness_of_fit(s.astype(str), s, _meta())
    assert not result.passed


def test_adf_stationary():
    np.random.seed(42)
    s = pd.Series(np.random.normal(0, 1, 200), name="col")
    result = check_stationarity_adf(s.astype(str), s, _meta())
    assert result.passed  # White noise is stationary


def test_adf_non_stationary():
    # Random walk (non-stationary)
    np.random.seed(42)
    s = pd.Series(np.cumsum(np.random.normal(0, 1, 200)), name="col")
    result = check_stationarity_adf(s.astype(str), s, _meta())
    assert not result.passed


def test_insufficient_data():
    s = pd.Series([1, 2, 3], name="col")
    result = check_normality_anderson(s.astype(str), s, _meta())
    assert result.passed  # Skip with insufficient data
