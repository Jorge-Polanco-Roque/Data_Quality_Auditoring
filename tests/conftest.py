"""Fixtures compartidos para tests."""

import sys
import os
import pytest
import pandas as pd
import numpy as np

# Asegurar que el directorio raíz del proyecto esté en el path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def empty_df():
    return pd.DataFrame()


@pytest.fixture
def single_row_df():
    return pd.DataFrame({"a": [1], "b": ["x"]})


@pytest.fixture
def all_nulls_df():
    return pd.DataFrame({"a": [None, None, None], "b": [np.nan, np.nan, np.nan]})


@pytest.fixture
def constant_df():
    return pd.DataFrame({"a": [42] * 100, "b": ["same"] * 100})


@pytest.fixture
def numeric_df():
    np.random.seed(42)
    return pd.DataFrame({
        "normal": np.random.normal(100, 15, 200),
        "skewed": np.random.exponential(10, 200),
        "uniform": np.random.uniform(0, 100, 200),
        "with_outliers": np.concatenate([np.random.normal(50, 5, 195), [500, -200, 999, -500, 1000]]),
    })


@pytest.fixture
def categorical_df():
    np.random.seed(42)
    return pd.DataFrame({
        "species": np.random.choice(["cat", "dog", "bird"], 150, p=[0.5, 0.3, 0.2]),
        "color": np.random.choice(["red", "blue", "green", "Red", "RED"], 150),
        "rare": np.random.choice(["A"] * 95 + ["B"] * 4 + ["C"], 150),
        "imbalanced": np.random.choice(["yes", "no"], 150, p=[0.96, 0.04]),
    })


@pytest.fixture
def date_df():
    dates = pd.date_range("2020-01-01", periods=100, freq="D")
    return pd.DataFrame({
        "date": dates.astype(str),
        "value": np.random.normal(50, 10, 100),
    })


@pytest.fixture
def mixed_nulls_df():
    np.random.seed(42)
    n = 200
    df = pd.DataFrame({
        "a": np.random.normal(100, 10, n),
        "b": np.random.normal(50, 5, n),
        "c": np.random.choice(["x", "y", "z"], n),
    })
    # Introduce correlated nulls
    mask = np.random.random(n) < 0.15
    df.loc[mask, "a"] = np.nan
    df.loc[mask, "b"] = np.nan  # Same rows as 'a'
    # Independent nulls in c
    df.loc[np.random.random(n) < 0.05, "c"] = np.nan
    return df


@pytest.fixture
def base_metadata(numeric_df):
    return {
        "_df_raw": numeric_df.astype(str),
        "_df": numeric_df,
        "_date_col": None,
    }
