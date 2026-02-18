import re
from collections import Counter

import pandas as pd

from models.check_result import CheckResult


def check_id_duplicates(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """ID_DUPLICATES: IDs duplicados (generalmente crítico)."""
    non_null = series_typed.dropna()
    if len(non_null) == 0:
        return CheckResult(
            check_id="ID_DUPLICATES", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.001, message="Sin datos",
        )

    dup_mask = non_null.duplicated(keep=False)
    dup_count = int(dup_mask.sum())
    n = len(non_null)
    pct = dup_count / n if n > 0 else 0.0

    if pct >= 0.01:
        severity = "CRITICAL"
    elif dup_count > 0:
        severity = "HIGH"
    else:
        severity = "PASS"

    samples = non_null[non_null.duplicated(keep="first")].head(5).astype(str).tolist()

    return CheckResult(
        check_id="ID_DUPLICATES", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.001,
        message=f"{dup_count:,} IDs duplicados ({pct:.1%})",
        affected_count=dup_count, affected_pct=pct, sample_values=samples,
    )


def check_id_format_consistency(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """ID_FORMAT_CONSISTENCY: patrón de formato inconsistente en IDs."""
    non_empty = series_raw.astype(str).str.strip()
    non_empty = non_empty[non_empty != ""]

    if len(non_empty) < 10:
        return CheckResult(
            check_id="ID_FORMAT_CONSISTENCY", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Datos insuficientes",
        )

    def _pattern(val):
        """Convierte un valor en su patrón: A=letra, 9=dígito, conserva separadores."""
        result = []
        for c in str(val):
            if c.isalpha():
                result.append("A")
            elif c.isdigit():
                result.append("9")
            else:
                result.append(c)
        return "".join(result)

    sample = non_empty.head(1000)
    patterns = sample.apply(_pattern)
    pattern_counts = patterns.value_counts(normalize=True)
    dominant_pct = float(pattern_counts.iloc[0])
    n_patterns = len(pattern_counts)

    if dominant_pct < 0.80 and n_patterns > 3:
        severity = "HIGH"
    elif dominant_pct < 0.90:
        severity = "MEDIUM"
    elif n_patterns > 2:
        severity = "LOW"
    else:
        severity = "PASS"

    # Muestras de patrones minoritarios
    if n_patterns > 1:
        minority_pattern = pattern_counts.index[1] if len(pattern_counts) > 1 else None
        minority_samples = sample[patterns == minority_pattern].head(3).tolist() if minority_pattern else []
    else:
        minority_samples = []

    return CheckResult(
        check_id="ID_FORMAT_CONSISTENCY", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(dominant_pct, 4), threshold=0.90,
        message=f"{n_patterns} patrones de formato detectados (dominante: {dominant_pct:.1%})",
        sample_values=minority_samples,
        metadata={
            "n_patterns": n_patterns,
            "dominant_pattern": pattern_counts.index[0],
            "dominant_pct": round(dominant_pct, 4),
            "top_patterns": {str(k): round(float(v), 4) for k, v in pattern_counts.head(5).items()},
        },
    )


def check_id_null(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """ID_NULL: nulos en columna de ID."""
    stripped = series_raw.astype(str).str.strip()
    is_null = (stripped == "") | (stripped.str.lower().isin({"nan", "null", "none", "na", "n/a"})) | series_typed.isna()
    null_count = int(is_null.sum())
    n = len(series_raw)
    pct = null_count / n if n > 0 else 0.0

    if null_count > 0:
        severity = "CRITICAL"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="ID_NULL", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.0,
        message=f"{null_count:,} IDs nulos ({pct:.1%})",
        affected_count=null_count, affected_pct=pct,
    )


ID_CHECKS = [
    {"check_id": "ID_DUPLICATES", "function": check_id_duplicates},
    {"check_id": "ID_FORMAT_CONSISTENCY", "function": check_id_format_consistency},
    {"check_id": "ID_NULL", "function": check_id_null},
]
