"""Tests para: executive summary, Excel export, trend analyzer, referential integrity, scoring configurable."""

import json
import os
import tempfile

import pandas as pd
import numpy as np
import pytest


def _make_report():
    """Crea un reporte mínimo para testing."""
    return {
        "report_metadata": {
            "generated_at": "2026-02-17T12:00:00",
            "file_analyzed": "test.csv",
            "total_rows": 100,
            "total_columns": 3,
            "encoding": "utf-8",
            "delimiter": ",",
        },
        "dataset_summary": {
            "health_score": 75.0,
            "health_grade": "B",
            "total_issues": 5,
            "issues_by_severity": {"CRITICAL": 0, "HIGH": 2, "MEDIUM": 2, "LOW": 1, "INFO": 0},
            "critical_columns": [],
            "clean_columns": ["col_c"],
        },
        "column_profiles": {
            "col_a": {
                "semantic_type": "NUMERIC_CONTINUOUS",
                "pandas_dtype": "float64",
                "n_unique": 50, "null_pct": 0.0,
                "health_score": 70, "health_grade": "C",
                "checks_run": 10, "checks_failed": 3,
                "issues": [
                    {"check_id": "OUTLIER_IQR", "severity": "MEDIUM", "message": "outliers", "value": 0.05, "threshold": 0.0, "affected_count": 5, "affected_pct": 0.05, "column": "col_a", "passed": False, "sample_values": [], "metadata": {}},
                ],
            },
        },
        "critical_issues": [
            {"check_id": "MEAN_SHIFT", "column": "col_a", "severity": "HIGH", "message": "shift detected", "affected_count": 0, "affected_pct": 0.0, "sample_values": []},
        ],
        "recommendations": [
            {"priority": 1, "category": "Outliers", "column": "col_a", "action": "Review outliers", "estimated_impact": "HIGH"},
        ],
        "statistical_summary": {"numeric_columns": {"col_a": {"mean": 50.0, "median": 48.0, "std": 10.0, "min": 5.0, "max": 95.0, "skewness": 0.1, "kurtosis": -0.2, "outlier_count_iqr": 3, "outlier_count_zscore": 1}}, "categorical_columns": {}, "date_columns": {}},
        "column_profiling": {},
    }


def test_executive_summary():
    from generate_report_executive import generate_executive_summary
    report = _make_report()
    md = generate_executive_summary(report)
    assert "Resumen Ejecutivo" in md
    assert "75.0/100" in md
    assert "VERDE" in md
    assert "MEAN_SHIFT" in md


def test_executive_summary_with_trend():
    from generate_report_executive import generate_executive_summary
    report = _make_report()
    report["quality_trend"] = {
        "previous_runs": 2,
        "trend": "STABLE",
        "trend_description": "Estable (+0.0 puntos)",
        "avg_previous_score": 74.5,
    }
    md = generate_executive_summary(report)
    assert "Tendencia" in md
    assert "Estable" in md


def test_excel_export(tmp_path):
    from generate_report_excel import generate_excel
    report = _make_report()
    path = str(tmp_path / "test.xlsx")
    generate_excel(report, path)
    assert os.path.exists(path)
    assert os.path.getsize(path) > 0


def test_excel_with_flagged_rows(tmp_path):
    from generate_report_excel import generate_excel
    report = _make_report()
    flagged = pd.DataFrame({
        "row_number": [1, 5, 10],
        "column": ["col_a", "col_a", "col_b"],
        "check_id": ["OUTLIER_IQR", "NULL_RATE", "NULL_RATE"],
        "severity": ["MEDIUM", "HIGH", "HIGH"],
        "value": ["999", "", ""],
        "detail": ["outlier", "null", "null"],
    })
    path = str(tmp_path / "test_flagged.xlsx")
    generate_excel(report, path, flagged_df=flagged)
    assert os.path.exists(path)


def test_scoring_configurable():
    from core.scoring_system import ScoringSystem
    from models.check_result import CheckResult

    results = [
        CheckResult(check_id="TEST", column="a", passed=False, severity="HIGH", value=0, threshold=0, message="test"),
        CheckResult(check_id="TEST2", column="a", passed=False, severity="MEDIUM", value=0, threshold=0, message="test"),
    ]
    null_pcts = {"a": 0.0}

    # Default scoring: HIGH=-10, MEDIUM=-5 → score=85
    scorer_default = ScoringSystem()
    result_default = scorer_default.calculate(results, null_pcts)
    assert result_default["column_scores"]["a"]["score"] == 85.0

    # Custom scoring: HIGH=-20, MEDIUM=-10 → score=70
    config = {"scoring": {"HIGH": 20, "MEDIUM": 10}, "column_weights": {}}
    scorer_custom = ScoringSystem(config=config)
    result_custom = scorer_custom.calculate(results, null_pcts)
    assert result_custom["column_scores"]["a"]["score"] == 70.0


def test_column_weights():
    from core.scoring_system import ScoringSystem
    from models.check_result import CheckResult

    results = [
        CheckResult(check_id="T1", column="important", passed=False, severity="HIGH", value=0, threshold=0, message="t"),
        CheckResult(check_id="T2", column="minor", passed=False, severity="HIGH", value=0, threshold=0, message="t"),
    ]
    null_pcts = {"important": 0.0, "minor": 0.0}

    # Con peso mayor para "important": el dataset score se sesga hacia "important"
    config = {"scoring": {}, "column_weights": {"important": 5.0, "minor": 1.0}}
    scorer = ScoringSystem(config=config)
    result = scorer.calculate(results, null_pcts)
    # Both have score 90, so dataset score should be 90 regardless of weight
    assert result["dataset_score"] == 90.0


def test_referential_integrity():
    from core.referential_integrity import ReferentialIntegrityChecker

    orders = pd.DataFrame({"order_id": [1, 2, 3], "customer_id": [10, 20, 999]})
    customers = pd.DataFrame({"id": [10, 20, 30]})

    rules = [{"child_table": "orders.csv", "child_column": "customer_id",
              "parent_table": "customers.csv", "parent_column": "id"}]

    checker = ReferentialIntegrityChecker(rules)
    results = checker.validate({"orders.csv": orders, "customers.csv": customers})
    assert len(results) == 1
    fk_result = results[0]
    assert not fk_result.passed
    assert fk_result.affected_count == 1  # 999 is orphan
    assert "999" in fk_result.sample_values


def test_referential_integrity_clean():
    from core.referential_integrity import ReferentialIntegrityChecker

    orders = pd.DataFrame({"customer_id": [10, 20, 30]})
    customers = pd.DataFrame({"id": [10, 20, 30, 40]})

    rules = [{"child_table": "orders.csv", "child_column": "customer_id",
              "parent_table": "customers.csv", "parent_column": "id"}]

    checker = ReferentialIntegrityChecker(rules)
    results = checker.validate({"orders.csv": orders, "customers.csv": customers})
    assert len(results) == 1
    assert results[0].passed


def test_trend_analyzer(tmp_path):
    from core.trend_analyzer import TrendAnalyzer

    # Crear una corrida falsa
    run_dir = tmp_path / "outputs" / "001_test"
    run_dir.mkdir(parents=True)
    report = {
        "report_metadata": {"generated_at": "2026-02-15T10:00:00", "total_rows": 100, "total_columns": 5},
        "dataset_summary": {"health_score": 80.0, "health_grade": "B", "total_issues": 10, "issues_by_severity": {}},
    }
    with open(run_dir / "report.json", "w") as f:
        json.dump(report, f)

    # Monkey-patch OUTPUTS_DIR
    import core.trend_analyzer as ta
    original = ta.OUTPUTS_DIR
    ta.OUTPUTS_DIR = str(tmp_path / "outputs")

    try:
        analyzer = TrendAnalyzer()
        trend = analyzer.build_trend_report("test", 70.0, "C")
        assert trend is not None
        assert trend["previous_runs"] == 1
        assert trend["trend"] == "DEGRADING"
        assert trend["delta_vs_previous"] == -10.0
    finally:
        ta.OUTPUTS_DIR = original


# ═══════════════════════════════════════════════════════════════════════════
# Tests: Expression validation (business rules security)
# ═══════════════════════════════════════════════════════════════════════════

def test_business_rule_rejects_injection():
    """Expresiones con tokens peligrosos deben ser rechazadas."""
    from core.business_rules import BusinessRulesEngine
    malicious_rules = [
        {"name": "Injection", "assertion": "__import__('os').system('ls')", "severity": "HIGH"},
    ]
    df = pd.DataFrame({"price": [10, 20]})
    engine = BusinessRulesEngine(malicious_rules)
    results = engine.evaluate(df)
    # Debe producir un resultado con error, no ejecutar código
    assert len(results) == 1
    assert results[0].metadata.get("error") is True


def test_business_rule_rejects_exec():
    """Expresiones con exec/eval deben ser rechazadas."""
    from core.business_rules import BusinessRulesEngine
    rules = [{"name": "Exec attack", "assertion": "exec('print(1)')", "severity": "HIGH"}]
    df = pd.DataFrame({"x": [1, 2]})
    engine = BusinessRulesEngine(rules)
    results = engine.evaluate(df)
    assert len(results) == 1
    assert results[0].metadata.get("error") is True


def test_business_rule_safe_expressions_still_work():
    """Expresiones legítimas siguen funcionando correctamente."""
    from core.business_rules import BusinessRulesEngine
    rules = [
        {"name": "Range check", "assertion": "price >= 0 and price <= 100", "severity": "MEDIUM"},
        {"name": "Comparison", "condition": "status == 'active'", "assertion": "balance > 0", "severity": "HIGH"},
    ]
    df = pd.DataFrame({
        "price": [10, 50, -5],
        "status": ["active", "inactive", "active"],
        "balance": [100, 0, 200],
    })
    engine = BusinessRulesEngine(rules)
    results = engine.evaluate(df)
    assert len(results) == 2
    # Range check: -5 falla
    assert not results[0].passed
    assert results[0].affected_count == 1
    # Comparison: ambos activos tienen balance > 0
    assert results[1].passed


# ═══════════════════════════════════════════════════════════════════════════
# Tests: Config validation
# ═══════════════════════════════════════════════════════════════════════════

def test_config_validation_invalid_severity_override():
    """Severidades inválidas en overrides deben rechazarse."""
    from core.config_loader import _validate_config, ConfigValidationError
    config = {"severity_overrides": {"NULL_RATE": "SUPER_CRITICAL"}}
    with pytest.raises(ConfigValidationError, match="severidad válida"):
        _validate_config(config)


def test_config_validation_invalid_threshold_type():
    """Thresholds con valores no numéricos deben rechazarse."""
    from core.config_loader import _validate_config, ConfigValidationError
    config = {"thresholds": {"NULL_RATE": {"CRITICAL": "alto"}}}
    with pytest.raises(ConfigValidationError, match="numérico"):
        _validate_config(config)


def test_config_validation_negative_scoring():
    """Scoring con valores negativos debe rechazarse."""
    from core.config_loader import _validate_config, ConfigValidationError
    config = {"scoring": {"CRITICAL": -5}}
    with pytest.raises(ConfigValidationError, match=">= 0"):
        _validate_config(config)


def test_config_validation_invalid_business_rule():
    """Business rules sin assertion deben rechazarse."""
    from core.config_loader import _validate_config, ConfigValidationError
    config = {"business_rules": [{"name": "Bad rule", "severity": "HIGH"}]}
    with pytest.raises(ConfigValidationError, match="assertion"):
        _validate_config(config)


def test_config_validation_valid_config():
    """Config válido no debe lanzar excepciones."""
    from core.config_loader import _validate_config
    config = {
        "thresholds": {"NULL_RATE": {"CRITICAL": 0.50, "HIGH": 0.20}},
        "disabled_checks": ["BENFORD_LAW"],
        "severity_overrides": {"TREND_CHANGE": "INFO"},
        "scoring": {"CRITICAL": 25, "HIGH": 10},
        "column_weights": {"amount": 3.0},
        "business_rules": [{"name": "Test", "assertion": "x > 0", "severity": "HIGH"}],
        "foreign_keys": [{"child_table": "a.csv", "child_column": "id",
                          "parent_table": "b.csv", "parent_column": "id"}],
    }
    _validate_config(config)  # No exception = pass


def test_config_validation_missing_fk_fields():
    """Foreign keys con campos faltantes deben rechazarse."""
    from core.config_loader import _validate_config, ConfigValidationError
    config = {"foreign_keys": [{"child_table": "a.csv"}]}
    with pytest.raises(ConfigValidationError, match="faltan campos"):
        _validate_config(config)
