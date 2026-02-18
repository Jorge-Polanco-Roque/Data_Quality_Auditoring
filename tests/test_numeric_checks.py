"""Tests para checks/numeric_checks.py"""

import pandas as pd
import numpy as np
from checks.numeric_checks import (
    check_outlier_iqr, check_outlier_zscore, check_outlier_modified_z,
    check_distribution_skew, check_distribution_kurtosis,
    check_negative_values, check_zero_values, check_value_range,
    check_normality_test,
)


def _meta():
    return {}


def test_outlier_iqr_clean():
    np.random.seed(42)
    s = pd.Series(np.random.normal(50, 5, 200), name="col")
    result = check_outlier_iqr(s.astype(str), s, _meta())
    # Most normal samples shouldn't have many outliers
    assert result.check_id == "OUTLIER_IQR"


def test_outlier_iqr_with_outliers():
    s = pd.Series(list(range(100)) + [9999, -9999], name="col")
    result = check_outlier_iqr(s.astype(str), s, _meta())
    assert result.affected_count >= 2


def test_outlier_zscore():
    s = pd.Series(list(range(100)) + [9999], name="col")
    result = check_outlier_zscore(s.astype(str), s, _meta())
    assert result.affected_count >= 1


def test_outlier_modified_z():
    s = pd.Series(list(range(100)) + [9999], name="col")
    result = check_outlier_modified_z(s.astype(str), s, _meta())
    assert result.check_id == "OUTLIER_MODIFIED_Z"


def test_skew_normal():
    np.random.seed(42)
    s = pd.Series(np.random.normal(0, 1, 500), name="col")
    result = check_distribution_skew(s.astype(str), s, _meta())
    assert result.passed  # Normal distribution has low skew


def test_skew_high():
    np.random.seed(42)
    s = pd.Series(np.random.exponential(1, 500), name="col")
    result = check_distribution_skew(s.astype(str), s, _meta())
    # Exponential has positive skew
    assert result.value > 0


def test_kurtosis():
    np.random.seed(42)
    s = pd.Series(np.random.normal(0, 1, 200), name="col")
    result = check_distribution_kurtosis(s.astype(str), s, _meta())
    assert result.check_id == "DISTRIBUTION_KURTOSIS"


def test_negative_values_present():
    s = pd.Series([1, -2, 3, -4, 5], name="col")
    result = check_negative_values(s.astype(str), s, _meta())
    assert result.affected_count == 2


def test_negative_values_absent():
    s = pd.Series([1, 2, 3, 4, 5], name="col")
    result = check_negative_values(s.astype(str), s, _meta())
    assert result.passed


def test_zero_values():
    s = pd.Series([0, 0, 0, 1, 2, 3, 4, 5, 6, 7], name="col")
    result = check_zero_values(s.astype(str), s, _meta())
    assert result.check_id == "ZERO_VALUES"


def test_value_range():
    s = pd.Series(list(range(200)) + [99999], name="col")
    result = check_value_range(s.astype(str), s, _meta())
    assert result.affected_count >= 1


def test_normality_normal():
    np.random.seed(42)
    s = pd.Series(np.random.normal(0, 1, 500), name="col")
    result = check_normality_test(s.astype(str), s, _meta())
    assert result.passed  # Should recognize normal data


def test_normality_non_normal():
    np.random.seed(42)
    s = pd.Series(np.random.exponential(1, 500), name="col")
    result = check_normality_test(s.astype(str), s, _meta())
    assert not result.passed


def test_insufficient_data():
    s = pd.Series([1, 2, 3], name="col")
    result = check_outlier_iqr(s.astype(str), s, _meta())
    assert result.passed  # Too few data points
