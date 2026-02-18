"""Tests para checks/benford_check.py"""

import pandas as pd
import numpy as np
from checks.benford_check import check_benford_law


def _meta():
    return {}


def test_benford_conforming():
    """Data that follows Benford's law (naturally occurring)."""
    np.random.seed(42)
    # Population data tends to follow Benford's law
    s = pd.Series(np.random.lognormal(5, 2, 1000), name="population")
    result = check_benford_law(s.astype(str), s, _meta())
    assert result.check_id == "BENFORD_LAW"
    # Lognormal data should roughly conform to Benford
    assert result.metadata.get("conformity") in ("altamente conforme", "conforme aceptable",
                                                   "marginalmente conforme")


def test_benford_non_conforming():
    """Uniform data does NOT follow Benford's law."""
    np.random.seed(42)
    s = pd.Series(np.random.uniform(100, 999, 1000), name="amounts")
    result = check_benford_law(s.astype(str), s, _meta())
    # Uniform should NOT conform
    assert result.metadata.get("conformity") in ("no conforme", "marginalmente conforme")


def test_benford_insufficient_data():
    s = pd.Series([1, 2, 3, 4, 5], name="col")
    result = check_benford_law(s.astype(str), s, _meta())
    assert result.passed  # Too few values


def test_benford_all_zeros():
    s = pd.Series([0] * 200, name="col")
    result = check_benford_law(s.astype(str), s, _meta())
    assert result.passed  # Zeros excluded, insufficient data
