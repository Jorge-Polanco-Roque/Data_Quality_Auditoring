"""
Checks de series temporales: autocorrelación, estacionalidad, changepoints.

Se activan cuando existe una columna de fecha y columnas numéricas.
"""

import numpy as np
import pandas as pd
from scipy import stats

from models.check_result import CheckResult
from models.semantic_type import SemanticType


def run_timeseries_checks(df, df_raw, column_types, date_col=None):
    """Ejecuta checks de series temporales. Retorna lista de CheckResult."""
    results = []

    # Encontrar columna de fecha
    if date_col and date_col in df.columns:
        dt_col = date_col
    else:
        dt_col = _find_date_column(df, column_types)

    if dt_col is None:
        return results

    dt = pd.to_datetime(df[dt_col], errors="coerce")
    if dt.dropna().empty:
        return results

    # Columnas numéricas
    num_cols = [c for c, t in column_types.items()
                if t in (SemanticType.NUMERIC_CONTINUOUS, SemanticType.NUMERIC_DISCRETE)
                and c in df.columns]

    for col in num_cols:
        s = pd.to_numeric(df[col], errors="coerce")
        valid = pd.DataFrame({"dt": dt, "val": s}).dropna().sort_values("dt")
        if len(valid) < 20:
            continue

        results.extend(_autocorrelation_check(valid["val"], col))
        results.extend(_seasonality_check(valid["val"], valid["dt"], col))
        results.extend(_changepoint_cusum(valid["val"], col))

    return results


def _find_date_column(df, column_types):
    for col, t in column_types.items():
        if t in (SemanticType.DATE, SemanticType.DATETIME):
            return col
    return None


# ---------------------------------------------------------------------------
# 1. Autocorrelación (ACF)
# ---------------------------------------------------------------------------

def _autocorrelation_check(series, col_name):
    results = []
    s = series.values
    n = len(s)
    if n < 30:
        return results

    try:
        from statsmodels.tsa.stattools import acf
        acf_values = acf(s, nlags=min(20, n // 2 - 1), fft=True)
    except Exception:
        # Fallback manual
        mean = np.mean(s)
        var = np.var(s)
        if var == 0:
            return results
        acf_values = []
        for lag in range(min(21, n // 2)):
            c = np.mean((s[:n - lag] - mean) * (s[lag:] - mean)) / var if lag > 0 else 1.0
            acf_values.append(c)
        acf_values = np.array(acf_values)

    # Significancia: ±1.96/√n
    threshold = 1.96 / np.sqrt(n)
    significant_lags = [i for i in range(1, len(acf_values)) if abs(acf_values[i]) > threshold]

    if len(significant_lags) > 5:
        severity = "MEDIUM"
    elif significant_lags:
        severity = "INFO"
    else:
        return results

    results.append(CheckResult(
        check_id="AUTOCORRELATION", column=col_name,
        passed=severity == "PASS", severity=severity,
        value=float(len(significant_lags)), threshold=threshold,
        message=f"{len(significant_lags)} lag(s) con autocorrelación significativa "
                f"(mayor lag significativo: {max(significant_lags) if significant_lags else 0})",
        sample_values=[f"Lag {l}: ACF={acf_values[l]:.3f}" for l in significant_lags[:5]],
        metadata={"significant_lags": significant_lags[:20],
                  "acf_values": [round(float(v), 4) for v in acf_values[:21]]},
    ))
    return results


# ---------------------------------------------------------------------------
# 2. Estacionalidad (STL decomposition o periodograma)
# ---------------------------------------------------------------------------

def _seasonality_check(series, dates, col_name):
    results = []
    s = series.values
    n = len(s)
    if n < 24:
        return results

    # Intentar STL decomposition
    try:
        from statsmodels.tsa.seasonal import STL
        # Crear serie temporal indexada
        ts = pd.Series(s, index=pd.DatetimeIndex(dates.values))
        ts = ts[~ts.index.duplicated(keep="first")]
        if len(ts) < 24:
            return results

        # Intentar detectar período
        period = _detect_period(ts)
        if period and period >= 2 and len(ts) >= 2 * period:
            stl = STL(ts, period=period, robust=True)
            result = stl.fit()

            seasonal_strength = 1.0 - (np.var(result.resid) / np.var(result.seasonal + result.resid))
            seasonal_strength = max(0.0, min(1.0, seasonal_strength))

            if seasonal_strength > 0.6:
                severity = "INFO"
                results.append(CheckResult(
                    check_id="SEASONALITY", column=col_name,
                    passed=True, severity=severity,
                    value=round(float(seasonal_strength), 4), threshold=0.6,
                    message=f"Estacionalidad detectada: fuerza={seasonal_strength:.2f}, "
                            f"período={period}",
                    metadata={"seasonal_strength": round(float(seasonal_strength), 4),
                              "detected_period": period},
                ))
    except Exception:
        # Fallback: usar periodograma simple
        try:
            from scipy.signal import periodogram
            freqs, psd = periodogram(s - np.mean(s))
            if len(psd) > 1:
                peak_idx = np.argmax(psd[1:]) + 1
                peak_freq = freqs[peak_idx]
                if peak_freq > 0:
                    peak_period = int(round(1.0 / peak_freq))
                    dominance = psd[peak_idx] / np.sum(psd[1:]) if np.sum(psd[1:]) > 0 else 0
                    if dominance > 0.15 and 2 <= peak_period <= n // 2:
                        results.append(CheckResult(
                            check_id="SEASONALITY", column=col_name,
                            passed=True, severity="INFO",
                            value=round(float(dominance), 4), threshold=0.15,
                            message=f"Posible estacionalidad: período≈{peak_period}, "
                                    f"dominancia espectral={dominance:.2f}",
                            metadata={"peak_period": peak_period,
                                      "spectral_dominance": round(float(dominance), 4)},
                        ))
        except Exception:
            pass

    return results


def _detect_period(ts):
    """Detecta período más probable a partir del índice temporal."""
    if len(ts) < 4:
        return None
    diffs = ts.index.to_series().diff().dropna()
    if len(diffs) == 0:
        return None
    median_diff = diffs.median()

    # Estimar período basado en granularidad
    if median_diff <= pd.Timedelta(hours=2):
        return 24  # hourly → daily
    elif median_diff <= pd.Timedelta(days=2):
        return 7  # daily → weekly
    elif median_diff <= pd.Timedelta(days=35):
        return 12  # monthly → yearly
    elif median_diff <= pd.Timedelta(days=100):
        return 4  # quarterly → yearly
    return None


# ---------------------------------------------------------------------------
# 3. Changepoint detection (CUSUM)
# ---------------------------------------------------------------------------

def _changepoint_cusum(series, col_name):
    results = []
    s = series.values
    n = len(s)
    if n < 30:
        return results

    mean = np.mean(s)
    std = np.std(s)
    if std == 0:
        return results

    # CUSUM acumulado
    cusum = np.cumsum(s - mean)

    # Detectar punto de máxima desviación
    max_idx = np.argmax(np.abs(cusum))
    max_cusum = abs(cusum[max_idx])

    # Umbral: basado en n y std
    threshold = 2 * std * np.sqrt(n)

    if max_cusum > threshold:
        # Calcular significancia con bootstrap
        changepoint_pct = max_idx / n

        if changepoint_pct < 0.1 or changepoint_pct > 0.9:
            severity = "LOW"
        else:
            severity = "MEDIUM"

        mean_before = np.mean(s[:max_idx + 1]) if max_idx > 0 else mean
        mean_after = np.mean(s[max_idx + 1:]) if max_idx < n - 1 else mean

        results.append(CheckResult(
            check_id="CHANGEPOINT_CUSUM", column=col_name,
            passed=False, severity=severity,
            value=round(float(max_cusum / threshold), 4), threshold=1.0,
            message=f"Changepoint detectado en posición {max_idx}/{n} ({changepoint_pct:.0%}): "
                    f"media antes={mean_before:.2f}, después={mean_after:.2f}",
            metadata={"changepoint_index": int(max_idx),
                      "changepoint_pct": round(float(changepoint_pct), 4),
                      "cusum_ratio": round(float(max_cusum / threshold), 4),
                      "mean_before": round(float(mean_before), 4),
                      "mean_after": round(float(mean_after), 4)},
        ))

    return results
