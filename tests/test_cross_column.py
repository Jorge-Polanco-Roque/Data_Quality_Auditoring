"""Tests para checks/cross_column_checks.py"""

import pandas as pd
import numpy as np
from models.semantic_type import SemanticType
from checks.cross_column_checks import run_cross_column_checks


def test_high_correlation():
    np.random.seed(42)
    x = np.random.normal(0, 1, 200)
    df = pd.DataFrame({"a": x, "b": x * 0.99 + np.random.normal(0, 0.01, 200), "c": np.random.normal(0, 1, 200)})
    types = {"a": SemanticType.NUMERIC_CONTINUOUS, "b": SemanticType.NUMERIC_CONTINUOUS,
             "c": SemanticType.NUMERIC_CONTINUOUS}
    results = run_cross_column_checks(df, df.astype(str), types)
    corr_checks = [r for r in results if r.check_id == "HIGH_CORRELATION"]
    assert len(corr_checks) >= 1


def test_no_correlation():
    np.random.seed(42)
    df = pd.DataFrame({"a": np.random.normal(0, 1, 200), "b": np.random.normal(0, 1, 200)})
    types = {"a": SemanticType.NUMERIC_CONTINUOUS, "b": SemanticType.NUMERIC_CONTINUOUS}
    results = run_cross_column_checks(df, df.astype(str), types)
    corr_checks = [r for r in results if r.check_id == "HIGH_CORRELATION"]
    assert len(corr_checks) == 0


def test_vif_multicollinear():
    np.random.seed(42)
    x = np.random.normal(0, 1, 200)
    df = pd.DataFrame({
        "a": x,
        "b": x + np.random.normal(0, 0.01, 200),
        "c": np.random.normal(0, 1, 200),
    })
    types = {c: SemanticType.NUMERIC_CONTINUOUS for c in df.columns}
    results = run_cross_column_checks(df, df.astype(str), types)
    vif_checks = [r for r in results if r.check_id == "MULTICOLLINEARITY_VIF"]
    assert len(vif_checks) >= 1


def test_single_column():
    df = pd.DataFrame({"a": [1, 2, 3]})
    types = {"a": SemanticType.NUMERIC_CONTINUOUS}
    results = run_cross_column_checks(df, df.astype(str), types)
    assert len(results) == 0  # Need at least 2 columns
