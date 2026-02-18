import pandas as pd
import numpy as np

from models.check_result import CheckResult


NULL_LIKE = {
    "", "null", "none", "nan", "na", "n/a", "n.a.", "-", "--", "---",
    "missing", "unknown", "undefined", "?", "nil", "#n/a", "not available",
    "not applicable", "sin dato", "sin información", "desconocido",
}

THRESHOLDS_NULL = {"CRITICAL": 0.50, "HIGH": 0.20, "MEDIUM": 0.05, "LOW": 0.01}
THRESHOLDS_DUPLICATE = {"CRITICAL": 0.10, "HIGH": 0.05, "MEDIUM": 0.01}


def _severity_from_thresholds(value, thresholds):
    for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        if sev in thresholds and value >= thresholds[sev]:
            return sev
    return "PASS"


def check_null_rate(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """NULL_RATE: % de nulos, NaN, y strings null-like."""
    lower = series_raw.astype(str).str.strip().str.lower()
    is_null = lower.isin(NULL_LIKE) | series_typed.isna()
    null_count = int(is_null.sum())
    n = len(series_raw)
    null_pct = null_count / n if n > 0 else 0.0

    severity = _severity_from_thresholds(null_pct, THRESHOLDS_NULL)
    passed = severity == "PASS"

    threshold = THRESHOLDS_NULL.get(severity, 0.0)
    samples = series_raw[is_null].head(5).tolist() if null_count > 0 else []

    return CheckResult(
        check_id="NULL_RATE",
        column=series_raw.name,
        passed=passed,
        severity=severity,
        value=round(null_pct, 4),
        threshold=threshold,
        message=f"{null_pct:.1%} de valores nulos o null-like",
        affected_count=null_count,
        affected_pct=null_pct,
        sample_values=samples,
    )


def check_duplicate_rows(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DUPLICATE_ROWS: filas completamente duplicadas (evaluado a nivel global)."""
    df_raw = metadata.get("_df_raw")
    if df_raw is None:
        return CheckResult(
            check_id="DUPLICATE_ROWS",
            column="__dataset__",
            passed=True,
            severity="PASS",
            value=0.0,
            threshold=0.0,
            message="No se pudo evaluar duplicados (sin acceso al DataFrame completo)",
        )

    n = len(df_raw)
    dup_mask = df_raw.duplicated(keep="first")
    dup_count = int(dup_mask.sum())
    dup_pct = dup_count / n if n > 0 else 0.0

    severity = _severity_from_thresholds(dup_pct, THRESHOLDS_DUPLICATE)
    passed = severity == "PASS"

    return CheckResult(
        check_id="DUPLICATE_ROWS",
        column="__dataset__",
        passed=passed,
        severity=severity,
        value=round(dup_pct, 4),
        threshold=THRESHOLDS_DUPLICATE.get(severity, 0.0),
        message=f"{dup_count:,} filas duplicadas ({dup_pct:.1%})",
        affected_count=dup_count,
        affected_pct=dup_pct,
    )


def check_whitespace_issues(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """WHITESPACE_ISSUES: valores con espacios leading/trailing."""
    raw_str = series_raw.astype(str)
    has_whitespace = raw_str != raw_str.str.strip()
    # Excluir los que ya son vacíos
    non_empty = raw_str.str.strip() != ""
    affected = has_whitespace & non_empty
    count = int(affected.sum())
    n = int(non_empty.sum())
    pct = count / n if n > 0 else 0.0

    if pct > 0.10:
        severity = "MEDIUM"
    elif pct > 0.01:
        severity = "LOW"
    elif count > 0:
        severity = "INFO"
    else:
        severity = "PASS"

    samples = series_raw[affected].head(5).apply(lambda x: repr(x)).tolist()

    return CheckResult(
        check_id="WHITESPACE_ISSUES",
        column=series_raw.name,
        passed=severity == "PASS",
        severity=severity,
        value=round(pct, 4),
        threshold=0.01,
        message=f"{count:,} valores con espacios leading/trailing ({pct:.1%})",
        affected_count=count,
        affected_pct=pct,
        sample_values=samples,
    )


def check_constant_column(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """CONSTANT_COLUMN: columna con un solo valor único."""
    non_null = series_typed.dropna()
    n_unique = non_null.nunique()
    is_constant = n_unique <= 1 and len(non_null) > 0

    if is_constant:
        unique_val = non_null.unique()[0] if len(non_null.unique()) > 0 else None
        return CheckResult(
            check_id="CONSTANT_COLUMN",
            column=series_raw.name,
            passed=False,
            severity="LOW",
            value=1.0,
            threshold=1.0,
            message=f"Columna constante: todos los valores son '{unique_val}'",
            affected_count=len(non_null),
            affected_pct=1.0,
            sample_values=[str(unique_val)],
        )

    return CheckResult(
        check_id="CONSTANT_COLUMN",
        column=series_raw.name,
        passed=True,
        severity="PASS",
        value=float(n_unique),
        threshold=1.0,
        message=f"Columna con {n_unique} valores únicos",
    )


def check_near_constant(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """NEAR_CONSTANT: un valor representa >95% de los registros."""
    non_null = series_typed.dropna()
    if len(non_null) == 0:
        return CheckResult(
            check_id="NEAR_CONSTANT",
            column=series_raw.name,
            passed=True,
            severity="PASS",
            value=0.0,
            threshold=0.95,
            message="Columna vacía",
        )

    value_counts = non_null.value_counts(normalize=True)
    top_pct = float(value_counts.iloc[0])
    top_val = value_counts.index[0]

    if top_pct >= 0.95:
        return CheckResult(
            check_id="NEAR_CONSTANT",
            column=series_raw.name,
            passed=False,
            severity="LOW",
            value=round(top_pct, 4),
            threshold=0.95,
            message=f"El valor '{top_val}' representa {top_pct:.1%} de los registros",
            affected_count=int(top_pct * len(non_null)),
            affected_pct=top_pct,
            sample_values=[str(top_val)],
        )

    return CheckResult(
        check_id="NEAR_CONSTANT",
        column=series_raw.name,
        passed=True,
        severity="PASS",
        value=round(top_pct, 4),
        threshold=0.95,
        message=f"Valor más frecuente representa {top_pct:.1%}",
    )


UNIVERSAL_CHECKS = [
    {"check_id": "NULL_RATE", "function": check_null_rate},
    {"check_id": "DUPLICATE_ROWS", "function": check_duplicate_rows},
    {"check_id": "WHITESPACE_ISSUES", "function": check_whitespace_issues},
    {"check_id": "CONSTANT_COLUMN", "function": check_constant_column},
    {"check_id": "NEAR_CONSTANT", "function": check_near_constant},
]
