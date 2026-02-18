import re
import numpy as np
import pandas as pd

from models.check_result import CheckResult


EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
PHONE_RE = re.compile(r"^[\+]?[\d\s\-\.\(\)]{7,20}$")

NULL_LIKE = {
    "", "null", "none", "nan", "na", "n/a", "n.a.", "-", "--", "---",
    "missing", "unknown", "undefined", "?", "nil", "#n/a", "not available",
    "not applicable", "sin dato", "sin información", "desconocido",
}


def check_email_format(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """EMAIL_FORMAT: emails que no cumplen RFC 5322 básico."""
    non_empty = series_raw.astype(str).str.strip()
    non_empty = non_empty[(non_empty != "") & (~non_empty.str.lower().isin(NULL_LIKE))]

    if len(non_empty) == 0:
        return CheckResult(
            check_id="EMAIL_FORMAT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin datos",
        )

    invalid_mask = ~non_empty.apply(lambda x: bool(EMAIL_RE.match(x)))
    count = int(invalid_mask.sum())
    pct = count / len(non_empty)

    if pct > 0.20:
        severity = "HIGH"
    elif pct > 0.05:
        severity = "MEDIUM"
    elif count > 0:
        severity = "LOW"
    else:
        severity = "PASS"

    samples = non_empty[invalid_mask].head(5).tolist()

    return CheckResult(
        check_id="EMAIL_FORMAT", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.05,
        message=f"{count:,} emails con formato inválido ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_phone_format(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """PHONE_FORMAT: teléfonos que no cumplen patrón esperado."""
    non_empty = series_raw.astype(str).str.strip()
    non_empty = non_empty[(non_empty != "") & (~non_empty.str.lower().isin(NULL_LIKE))]

    if len(non_empty) == 0:
        return CheckResult(
            check_id="PHONE_FORMAT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin datos",
        )

    invalid_mask = ~non_empty.apply(lambda x: bool(PHONE_RE.match(x)))
    count = int(invalid_mask.sum())
    pct = count / len(non_empty)

    if pct > 0.20:
        severity = "HIGH"
    elif pct > 0.05:
        severity = "MEDIUM"
    elif count > 0:
        severity = "LOW"
    else:
        severity = "PASS"

    samples = non_empty[invalid_mask].head(5).tolist()

    return CheckResult(
        check_id="PHONE_FORMAT", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.05,
        message=f"{count:,} teléfonos con formato inválido ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_length_outliers(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """LENGTH_OUTLIERS: longitud de string muy fuera del rango típico."""
    non_empty = series_raw.astype(str).str.strip()
    non_empty = non_empty[non_empty != ""]

    if len(non_empty) < 10:
        return CheckResult(
            check_id="LENGTH_OUTLIERS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Datos insuficientes",
        )

    lengths = non_empty.str.len()
    q1, q3 = lengths.quantile(0.25), lengths.quantile(0.75)
    iqr = q3 - q1

    if iqr == 0:
        # Si todos tienen la misma longitud, cualquier diferencia es outlier
        median_len = lengths.median()
        mask = lengths != median_len
    else:
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        mask = (lengths < lower) | (lengths > upper)

    count = int(mask.sum())
    pct = count / len(non_empty)

    if pct > 0.10:
        severity = "MEDIUM"
    elif pct > 0.02:
        severity = "LOW"
    elif count > 0:
        severity = "INFO"
    else:
        severity = "PASS"

    samples = non_empty[mask].head(5).tolist()

    return CheckResult(
        check_id="LENGTH_OUTLIERS", column=series_raw.name,
        passed=severity in ("PASS", "INFO"), severity=severity,
        value=round(pct, 4), threshold=0.02,
        message=f"{count:,} valores con longitud atípica ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
        metadata={"median_length": float(lengths.median()), "mean_length": round(float(lengths.mean()), 1)},
    )


def check_null_like_strings(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """NULL_LIKE_STRINGS: strings que son N/A, null, none, NA, NaN, -, etc."""
    raw_str = series_raw.astype(str).str.strip()
    # Excluir los vacíos reales (ya cubiertos por NULL_RATE)
    non_empty = raw_str[raw_str != ""]

    if len(non_empty) == 0:
        return CheckResult(
            check_id="NULL_LIKE_STRINGS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin datos",
        )

    null_like_mask = non_empty.str.lower().isin(NULL_LIKE - {""})
    count = int(null_like_mask.sum())
    pct = count / len(non_empty)

    if pct > 0.10:
        severity = "HIGH"
    elif pct > 0.02:
        severity = "MEDIUM"
    elif count > 0:
        severity = "LOW"
    else:
        severity = "PASS"

    samples = non_empty[null_like_mask].unique()[:5].tolist()

    return CheckResult(
        check_id="NULL_LIKE_STRINGS", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.02,
        message=f"{count:,} strings null-like ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_truncation_signs(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """TRUNCATION_SIGNS: valores que terminan abruptamente (posible truncación)."""
    non_empty = series_raw.astype(str).str.strip()
    non_empty = non_empty[(non_empty != "") & (non_empty.str.len() > 5)]

    if len(non_empty) < 10:
        return CheckResult(
            check_id="TRUNCATION_SIGNS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Datos insuficientes",
        )

    # Señales de truncación: terminan en "...", se cortan a longitud fija, terminan en medio de palabra
    max_len = non_empty.str.len().max()
    at_max = non_empty.str.len() == max_len
    ends_ellipsis = non_empty.str.endswith("...")
    ends_abrupt = non_empty.str.match(r".*[a-záéíóúñ]{2,}$", case=False) & at_max

    truncated = ends_ellipsis | ends_abrupt
    count = int(truncated.sum())
    pct = count / len(non_empty)

    if pct > 0.05:
        severity = "MEDIUM"
    elif count > 0:
        severity = "LOW"
    else:
        severity = "PASS"

    samples = non_empty[truncated].head(5).tolist()

    return CheckResult(
        check_id="TRUNCATION_SIGNS", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.05,
        message=f"{count:,} valores con posible truncación ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


# Checks genéricos de texto (para HIGH_CARDINALITY y otros strings)
TEXT_CHECKS_GENERIC = [
    {"check_id": "LENGTH_OUTLIERS", "function": check_length_outliers},
    {"check_id": "NULL_LIKE_STRINGS", "function": check_null_like_strings},
    {"check_id": "TRUNCATION_SIGNS", "function": check_truncation_signs},
]

# Checks específicos de formato (solo para EMAIL / PHONE)
EMAIL_CHECKS = [
    {"check_id": "EMAIL_FORMAT", "function": check_email_format},
]

PHONE_CHECKS = [
    {"check_id": "PHONE_FORMAT", "function": check_phone_format},
]

# Exportar todo junto para backward-compat
TEXT_CHECKS = TEXT_CHECKS_GENERIC + EMAIL_CHECKS + PHONE_CHECKS
