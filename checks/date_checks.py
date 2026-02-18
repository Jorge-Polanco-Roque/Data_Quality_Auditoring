from datetime import datetime, timedelta
from collections import Counter

import numpy as np
import pandas as pd
from scipy import stats

from models.check_result import CheckResult


DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
    "%Y/%m/%d", "%d.%m.%Y", "%Y%m%d",
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
    "%d %b %Y", "%B %d, %Y", "%d de %B de %Y",
]


def _parse_date(val_str):
    """Intenta parsear una fecha con los formatos conocidos. Retorna (datetime, formato) o (None, None)."""
    val_str = str(val_str).strip()
    if not val_str:
        return None, None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(val_str, fmt), fmt
        except ValueError:
            continue
    try:
        from dateutil import parser as date_parser
        return date_parser.parse(val_str, fuzzy=False), "dateutil"
    except (ValueError, OverflowError):
        return None, None


def _to_datetime_series(series_typed: pd.Series) -> pd.Series:
    """Convierte a datetime, retornando solo valores válidos."""
    return pd.to_datetime(series_typed, errors="coerce").dropna()


def check_date_null_rate(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_NULL_RATE: nulos en columna de fecha."""
    dt = pd.to_datetime(series_typed, errors="coerce")
    null_count = int(dt.isna().sum())
    n = len(series_typed)
    pct = null_count / n if n > 0 else 0.0

    if pct >= 0.50:
        severity = "CRITICAL"
    elif pct >= 0.20:
        severity = "HIGH"
    elif pct >= 0.05:
        severity = "MEDIUM"
    elif pct > 0:
        severity = "LOW"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="DATE_NULL_RATE", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.05,
        message=f"{null_count:,} fechas nulas o no parseables ({pct:.1%})",
        affected_count=null_count, affected_pct=pct,
    )


def check_date_format_mix(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_FORMAT_MIX: múltiples formatos de fecha en la misma columna."""
    non_empty = series_raw.astype(str).str.strip()
    non_empty = non_empty[non_empty != ""]

    sample = non_empty.head(500)
    format_counter = Counter()

    for val in sample:
        _, fmt = _parse_date(val)
        if fmt:
            format_counter[fmt] += 1

    n_formats = len(format_counter)

    if n_formats >= 4:
        severity = "CRITICAL"
    elif n_formats >= 2:
        severity = "HIGH"
    else:
        severity = "PASS"

    formats_found = dict(format_counter.most_common(5))
    samples = []
    for fmt in list(format_counter.keys())[:5]:
        for val in sample:
            _, f = _parse_date(val)
            if f == fmt:
                samples.append(str(val))
                break

    return CheckResult(
        check_id="DATE_FORMAT_MIX", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=float(n_formats), threshold=2.0,
        message=f"{n_formats} formato(s) de fecha detectados",
        sample_values=samples[:5],
        metadata={"formats_found": formats_found},
    )


def check_date_future(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_FUTURE: fechas futuras."""
    dt = _to_datetime_series(series_typed)
    if len(dt) == 0:
        return CheckResult(
            check_id="DATE_FUTURE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin fechas válidas",
        )

    now = pd.Timestamp.now()
    future_mask = dt > now
    count = int(future_mask.sum())
    pct = count / len(dt)

    if pct > 0.10:
        severity = "HIGH"
    elif count > 0:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    samples = dt[future_mask].head(5).astype(str).tolist()

    return CheckResult(
        check_id="DATE_FUTURE", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.0,
        message=f"{count:,} fechas futuras ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_date_ancient(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_ANCIENT: fechas antes de 1900."""
    dt = _to_datetime_series(series_typed)
    if len(dt) == 0:
        return CheckResult(
            check_id="DATE_ANCIENT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin fechas válidas",
        )

    cutoff = pd.Timestamp("1900-01-01")
    ancient_mask = dt < cutoff
    count = int(ancient_mask.sum())
    pct = count / len(dt)

    severity = "HIGH" if count > 0 else "PASS"
    samples = dt[ancient_mask].head(5).astype(str).tolist()

    return CheckResult(
        check_id="DATE_ANCIENT", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.0,
        message=f"{count:,} fechas anteriores a 1900 ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_date_sequence_gaps(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_SEQUENCE_GAPS: gaps inesperados en series temporales."""
    dt = _to_datetime_series(series_typed).sort_values()
    if len(dt) < 10:
        return CheckResult(
            check_id="DATE_SEQUENCE_GAPS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Datos insuficientes",
        )

    diffs = dt.diff().dropna()
    if len(diffs) == 0:
        return CheckResult(
            check_id="DATE_SEQUENCE_GAPS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin diferencias calculables",
        )

    median_diff = diffs.median()
    if median_diff == pd.Timedelta(0):
        return CheckResult(
            check_id="DATE_SEQUENCE_GAPS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Diferencia mediana entre fechas es 0",
        )

    # Gaps significativos: > 3x la mediana
    gap_mask = diffs > (3 * median_diff)
    gap_count = int(gap_mask.sum())

    if gap_count > 10:
        severity = "HIGH"
    elif gap_count > 3:
        severity = "MEDIUM"
    elif gap_count > 0:
        severity = "LOW"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="DATE_SEQUENCE_GAPS", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=float(gap_count), threshold=3.0,
        message=f"{gap_count} gaps temporales significativos (>3x mediana de {median_diff})",
        affected_count=gap_count,
        metadata={"median_diff": str(median_diff), "gaps_found": gap_count},
    )


def check_date_duplicates(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_DUPLICATES: fechas duplicadas."""
    dt = _to_datetime_series(series_typed)
    if len(dt) == 0:
        return CheckResult(
            check_id="DATE_DUPLICATES", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin fechas válidas",
        )

    dup_mask = dt.duplicated(keep=False)
    dup_count = int(dup_mask.sum())
    pct = dup_count / len(dt)

    if pct > 0.50:
        severity = "MEDIUM"
    elif pct > 0.10:
        severity = "LOW"
    elif dup_count > 0:
        severity = "INFO"
    else:
        severity = "PASS"

    samples = dt[dup_mask].head(5).astype(str).tolist()

    return CheckResult(
        check_id="DATE_DUPLICATES", column=series_raw.name,
        passed=severity in ("PASS", "INFO"), severity=severity,
        value=round(pct, 4), threshold=0.10,
        message=f"{dup_count:,} fechas duplicadas ({pct:.1%})",
        affected_count=dup_count, affected_pct=pct, sample_values=samples,
    )


def check_date_monotonicity(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_MONOTONICITY: verifica si la columna está ordenada."""
    dt = _to_datetime_series(series_typed)
    if len(dt) < 3:
        return CheckResult(
            check_id="DATE_MONOTONICITY", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Datos insuficientes",
        )

    is_increasing = dt.is_monotonic_increasing
    is_decreasing = dt.is_monotonic_decreasing

    if is_increasing or is_decreasing:
        direction = "ascendente" if is_increasing else "descendente"
        return CheckResult(
            check_id="DATE_MONOTONICITY", column=series_raw.name,
            passed=True, severity="PASS",
            value=1.0, threshold=0.0,
            message=f"Columna de fecha es monótonamente {direction}",
        )

    return CheckResult(
        check_id="DATE_MONOTONICITY", column=series_raw.name,
        passed=True, severity="INFO",
        value=0.0, threshold=0.0,
        message="Columna de fecha no está ordenada monótonamente",
    )


def check_date_invalid_parsed(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DATE_INVALID_PARSED: valores que no pudieron parsearse como fecha."""
    non_empty = series_raw.astype(str).str.strip()
    non_empty = non_empty[(non_empty != "") & (non_empty.str.lower() != "nan")]

    dt = pd.to_datetime(series_typed, errors="coerce")
    originally_not_null = non_empty.index
    invalid_mask = dt.loc[originally_not_null].isna()
    count = int(invalid_mask.sum())
    n = len(originally_not_null)
    pct = count / n if n > 0 else 0.0

    if pct > 0.20:
        severity = "HIGH"
    elif pct > 0.05:
        severity = "MEDIUM"
    elif count > 0:
        severity = "LOW"
    else:
        severity = "PASS"

    samples = series_raw.loc[invalid_mask[invalid_mask].index].head(5).tolist()

    return CheckResult(
        check_id="DATE_INVALID_PARSED", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.05,
        message=f"{count:,} valores no parseables como fecha ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_temporal_drift(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """TEMPORAL_DRIFT: cambio en distribución de valores numéricos a lo largo del tiempo."""
    date_col = metadata.get("_date_col")
    df = metadata.get("_df")

    if date_col is None or df is None:
        return CheckResult(
            check_id="TEMPORAL_DRIFT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.05,
            message="No hay columna de fecha disponible para análisis de drift",
        )

    numeric_s = pd.to_numeric(series_typed, errors="coerce").dropna()
    if len(numeric_s) < 100:
        return CheckResult(
            check_id="TEMPORAL_DRIFT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.05,
            message="Datos insuficientes para análisis de drift temporal",
        )

    dates = pd.to_datetime(df[date_col], errors="coerce")
    valid = numeric_s.index.intersection(dates.dropna().index)
    if len(valid) < 100:
        return CheckResult(
            check_id="TEMPORAL_DRIFT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.05,
            message="Datos insuficientes tras alinear con columna de fecha",
        )

    combined = pd.DataFrame({"value": numeric_s.loc[valid], "date": dates.loc[valid]}).sort_values("date")
    q1_idx = len(combined) // 4
    q4_start = 3 * len(combined) // 4

    q1_values = combined["value"].iloc[:q1_idx]
    q4_values = combined["value"].iloc[q4_start:]

    if len(q1_values) < 10 or len(q4_values) < 10:
        return CheckResult(
            check_id="TEMPORAL_DRIFT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.05,
            message="Cuartiles temporales demasiado pequeños",
        )

    ks_stat, p_value = stats.ks_2samp(q1_values, q4_values)
    has_drift = p_value < 0.05

    if has_drift and p_value < 0.001:
        severity = "HIGH"
    elif has_drift:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="TEMPORAL_DRIFT", column=series_raw.name,
        passed=not has_drift, severity=severity,
        value=round(p_value, 6), threshold=0.05,
        message=f"KS test Q1 vs Q4: p={p_value:.4f} — {'drift significativo' if has_drift else 'sin drift'}",
        metadata={"ks_statistic": round(float(ks_stat), 6), "p_value": round(float(p_value), 6)},
    )


DATE_CHECKS = [
    {"check_id": "DATE_NULL_RATE", "function": check_date_null_rate},
    {"check_id": "DATE_FORMAT_MIX", "function": check_date_format_mix},
    {"check_id": "DATE_FUTURE", "function": check_date_future},
    {"check_id": "DATE_ANCIENT", "function": check_date_ancient},
    {"check_id": "DATE_SEQUENCE_GAPS", "function": check_date_sequence_gaps},
    {"check_id": "DATE_DUPLICATES", "function": check_date_duplicates},
    {"check_id": "DATE_MONOTONICITY", "function": check_date_monotonicity},
    {"check_id": "DATE_INVALID_PARSED", "function": check_date_invalid_parsed},
    {"check_id": "TEMPORAL_DRIFT", "function": check_temporal_drift},
]
