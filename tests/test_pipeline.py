"""Integration tests para el pipeline completo."""

import os
import json
import tempfile
import pandas as pd
import numpy as np

from core.data_loader import DataLoader
from core.type_detector import TypeDetector
from core.check_engine import CheckEngine
from core.scoring_system import ScoringSystem
from core.report_builder import ReportBuilder
from generate_report_md import generate_markdown


def _create_temp_csv(df, filename="test.csv"):
    path = os.path.join(tempfile.gettempdir(), filename)
    df.to_csv(path, index=False)
    return path


def test_full_pipeline_numeric():
    """Pipeline completo con datos numéricos."""
    np.random.seed(42)
    df = pd.DataFrame({
        "a": np.random.normal(100, 15, 100),
        "b": np.random.uniform(0, 100, 100),
    })
    path = _create_temp_csv(df)

    loader = DataLoader()
    df_raw, df_loaded, meta = loader.load(path)
    assert meta["n_rows"] == 100
    assert meta["n_cols"] == 2

    detector = TypeDetector()
    types = detector.detect(df_raw, df_loaded)
    assert all(t.value.startswith("NUMERIC") for t in types.values())

    engine = CheckEngine()
    results = engine.run_all(df_raw, df_loaded, types)
    assert len(results) > 0

    null_pcts = {col: float(df_loaded[col].isna().mean()) for col in df_loaded.columns}
    scorer = ScoringSystem()
    scoring = scorer.calculate(results, null_pcts)
    assert 0 <= scoring["dataset_score"] <= 100

    builder = ReportBuilder()
    report = builder.build(results, scoring, types, meta, df_loaded)
    assert "dataset_summary" in report
    assert "column_profiles" in report

    os.unlink(path)


def test_full_pipeline_categorical():
    """Pipeline completo con datos categóricos."""
    df = pd.DataFrame({
        "species": ["cat", "dog", "bird"] * 50,
        "color": ["red", "blue", "green"] * 50,
    })
    path = _create_temp_csv(df)

    loader = DataLoader()
    df_raw, df_loaded, meta = loader.load(path)

    detector = TypeDetector()
    types = detector.detect(df_raw, df_loaded)

    engine = CheckEngine()
    results = engine.run_all(df_raw, df_loaded, types)

    null_pcts = {col: float(df_loaded[col].isna().mean()) for col in df_loaded.columns}
    scorer = ScoringSystem()
    scoring = scorer.calculate(results, null_pcts)

    builder = ReportBuilder()
    report = builder.build(results, scoring, types, meta, df_loaded)
    assert report["dataset_summary"]["health_score"] > 0

    os.unlink(path)


def test_markdown_generation():
    """Test que generate_markdown produce output válido."""
    report = {
        "report_metadata": {
            "generated_at": "2026-01-01T00:00:00",
            "file_analyzed": "test.csv",
            "total_rows": 100,
            "total_columns": 2,
            "encoding": "utf-8",
            "delimiter": "','",
        },
        "dataset_summary": {
            "health_score": 95.0,
            "health_grade": "A",
            "total_issues": 1,
            "issues_by_severity": {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 1, "LOW": 0, "INFO": 0},
            "critical_columns": [],
            "clean_columns": ["col_a"],
        },
        "column_profiles": {
            "col_a": {
                "semantic_type": "NUMERIC_CONTINUOUS",
                "pandas_dtype": "float64",
                "n_unique": 100,
                "null_pct": 0.0,
                "health_score": 95.0,
                "health_grade": "A",
                "checks_run": 10,
                "checks_failed": 1,
                "issues": [{"check_id": "OUTLIER_IQR", "severity": "MEDIUM",
                           "message": "3 outliers IQR (3.0%)"}],
            }
        },
        "critical_issues": [],
        "recommendations": [],
        "statistical_summary": {"numeric_columns": {}, "categorical_columns": {}, "date_columns": {}},
    }

    md = generate_markdown(report)
    assert "# " in md  # Has header
    assert "test.csv" in md
    assert "95.0" in md
    assert "OUTLIER_IQR" in md


def test_empty_csv():
    """Pipeline con CSV que tiene solo headers debería fallar gracefully."""
    path = os.path.join(tempfile.gettempdir(), "empty_test.csv")
    with open(path, "w") as f:
        f.write("col_a,col_b\n")

    loader = DataLoader()
    try:
        df_raw, df_loaded, meta = loader.load(path)
        # Should raise ValueError for header-only file
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "sin datos" in str(e).lower()
    finally:
        os.unlink(path)


def test_binary_file_detection():
    """Pipeline debería rechazar archivos binarios."""
    path = os.path.join(tempfile.gettempdir(), "binary_test.csv")
    with open(path, "wb") as f:
        f.write(b"\x00" * 1000)

    loader = DataLoader()
    try:
        df_raw, df_loaded, meta = loader.load(path)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "binario" in str(e).lower()
    finally:
        os.unlink(path)
