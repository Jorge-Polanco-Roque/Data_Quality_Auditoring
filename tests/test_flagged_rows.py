"""Tests para Flagged Rows Exporter."""

import pandas as pd
import numpy as np
import pytest

from core.flagged_rows import FlaggedRowsExporter
from models.check_result import CheckResult


def test_null_flagging():
    df_raw = pd.DataFrame({"a": ["1", None, "3", None, "5"]})
    df = pd.DataFrame({"a": [1.0, np.nan, 3.0, np.nan, 5.0]})
    results = [
        CheckResult(
            check_id="NULL_RATE", column="a", passed=False, severity="HIGH",
            value=0.4, threshold=0.05, message="40% nulls",
            affected_count=2, affected_pct=0.4,
        )
    ]
    exporter = FlaggedRowsExporter()
    flagged = exporter.collect_flagged_rows(df_raw, df, results, {"a": None})
    assert len(flagged) == 2
    assert list(flagged["row_number"]) == [2, 4]


def test_outlier_flagging():
    values = [10, 11, 12, 13, 14, 15, 100, 10, 11, 12]
    df_raw = pd.DataFrame({"a": [str(v) for v in values]})
    df = pd.DataFrame({"a": values})
    results = [
        CheckResult(
            check_id="OUTLIER_IQR", column="a", passed=False, severity="MEDIUM",
            value=0.1, threshold=0.0, message="outliers",
            affected_count=1, affected_pct=0.1,
        )
    ]
    exporter = FlaggedRowsExporter()
    flagged = exporter.collect_flagged_rows(df_raw, df, results, {"a": None})
    assert len(flagged) > 0
    assert 7 in flagged["row_number"].values  # row 7 (index 6, value=100)


def test_no_issues():
    df_raw = pd.DataFrame({"a": ["1", "2", "3"]})
    df = pd.DataFrame({"a": [1, 2, 3]})
    results = [
        CheckResult(
            check_id="NULL_RATE", column="a", passed=True, severity="PASS",
            value=0.0, threshold=0.05, message="clean",
        )
    ]
    exporter = FlaggedRowsExporter()
    flagged = exporter.collect_flagged_rows(df_raw, df, results, {"a": None})
    assert len(flagged) == 0


def test_export_csv(tmp_path):
    df_raw = pd.DataFrame({"a": ["1", None, "3"]})
    df = pd.DataFrame({"a": [1.0, np.nan, 3.0]})
    results = [
        CheckResult(
            check_id="NULL_RATE", column="a", passed=False, severity="HIGH",
            value=0.33, threshold=0.05, message="nulls found",
            affected_count=1, affected_pct=0.33,
        )
    ]
    exporter = FlaggedRowsExporter()
    flagged = exporter.collect_flagged_rows(df_raw, df, results, {"a": None})
    path = str(tmp_path / "flagged.csv")
    exporter.export(flagged, path)

    loaded = pd.read_csv(path)
    assert len(loaded) > 0
    assert "row_number" in loaded.columns
