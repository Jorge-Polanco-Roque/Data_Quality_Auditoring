import numpy as np
import pandas as pd
from scipy import stats

from models.check_result import CheckResult


THRESHOLDS_OUTLIER_IQR = {"CRITICAL": 0.10, "HIGH": 0.05, "MEDIUM": 0.02, "LOW": 0.005}
THRESHOLDS_OUTLIER_ZSCORE = {"CRITICAL": 0.05, "HIGH": 0.02, "MEDIUM": 0.01}
THRESHOLDS_SKEWNESS = {"HIGH": 3.0, "MEDIUM": 2.0, "LOW": 1.0}
THRESHOLDS_ZERO = {"HIGH": 0.30, "MEDIUM": 0.10}
THRESHOLDS_TREND = {"CRITICAL": 3.0, "HIGH": 2.5, "MEDIUM": 2.0}


def _severity_from_thresholds(value, thresholds):
    for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        if sev in thresholds and value >= thresholds[sev]:
            return sev
    return "PASS"


def _numeric_series(series_typed: pd.Series) -> pd.Series:
    """Convierte a numérico y elimina NaN."""
    s = pd.to_numeric(series_typed, errors="coerce").dropna()
    return s


def check_outlier_iqr(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """OUTLIER_IQR: valores fuera de 1.5xIQR (Tukey)."""
    s = _numeric_series(series_typed)
    if len(s) < 10:
        return CheckResult(
            check_id="OUTLIER_IQR", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para detección de outliers IQR",
        )

    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return CheckResult(
            check_id="OUTLIER_IQR", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="IQR es 0, no se puede calcular outliers",
        )

    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    mask = (s < lower) | (s > upper)
    count = int(mask.sum())
    pct = count / len(s) if len(s) > 0 else 0.0

    severity = _severity_from_thresholds(pct, THRESHOLDS_OUTLIER_IQR)
    samples = s[mask].head(5).tolist()

    return CheckResult(
        check_id="OUTLIER_IQR", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=THRESHOLDS_OUTLIER_IQR.get(severity, 0.0),
        message=f"{count:,} outliers IQR ({pct:.1%}), rango válido: [{lower:.2f}, {upper:.2f}]",
        affected_count=count, affected_pct=pct, sample_values=samples,
        metadata={"Q1": round(float(q1), 4), "Q3": round(float(q3), 4), "IQR": round(float(iqr), 4)},
    )


def check_outlier_zscore(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """OUTLIER_ZSCORE: valores con |z-score| > 3."""
    s = _numeric_series(series_typed)
    if len(s) < 10:
        return CheckResult(
            check_id="OUTLIER_ZSCORE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para detección de outliers Z-score",
        )

    mean, std = s.mean(), s.std()
    if std == 0:
        return CheckResult(
            check_id="OUTLIER_ZSCORE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Desviación estándar es 0",
        )

    z_scores = ((s - mean) / std).abs()
    mask = z_scores > 3
    count = int(mask.sum())
    pct = count / len(s)

    severity = _severity_from_thresholds(pct, THRESHOLDS_OUTLIER_ZSCORE)
    samples = s[mask].head(5).tolist()

    return CheckResult(
        check_id="OUTLIER_ZSCORE", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=THRESHOLDS_OUTLIER_ZSCORE.get(severity, 0.0),
        message=f"{count:,} outliers Z-score ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_outlier_modified_z(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """OUTLIER_MODIFIED_Z: Modified Z-score con MAD, robusto para distribuciones sesgadas."""
    s = _numeric_series(series_typed)
    if len(s) < 10:
        return CheckResult(
            check_id="OUTLIER_MODIFIED_Z", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes",
        )

    median = s.median()
    mad = (s - median).abs().median()
    if mad == 0:
        return CheckResult(
            check_id="OUTLIER_MODIFIED_Z", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="MAD es 0, no se puede calcular modified Z-score",
        )

    modified_z = 0.6745 * (s - median) / mad
    mask = modified_z.abs() > 3.5
    count = int(mask.sum())
    pct = count / len(s)

    severity = _severity_from_thresholds(pct, THRESHOLDS_OUTLIER_ZSCORE)
    samples = s[mask].head(5).tolist()

    return CheckResult(
        check_id="OUTLIER_MODIFIED_Z", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=THRESHOLDS_OUTLIER_ZSCORE.get(severity, 0.0),
        message=f"{count:,} outliers Modified Z-score ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_distribution_skew(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DISTRIBUTION_SKEW: skewness excesiva (distribución muy asimétrica)."""
    s = _numeric_series(series_typed)
    if len(s) < 20:
        return CheckResult(
            check_id="DISTRIBUTION_SKEW", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para evaluar skewness",
        )

    skew = float(s.skew())
    abs_skew = abs(skew)
    severity = _severity_from_thresholds(abs_skew, THRESHOLDS_SKEWNESS)
    direction = "positiva (cola derecha)" if skew > 0 else "negativa (cola izquierda)"

    return CheckResult(
        check_id="DISTRIBUTION_SKEW", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(skew, 4), threshold=THRESHOLDS_SKEWNESS.get(severity, 0.0),
        message=f"Skewness {direction}: {skew:.2f}",
        metadata={"skewness": round(skew, 4), "direction": direction},
    )


def check_distribution_kurtosis(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """DISTRIBUTION_KURTOSIS: kurtosis excesiva (colas pesadas)."""
    s = _numeric_series(series_typed)
    if len(s) < 20:
        return CheckResult(
            check_id="DISTRIBUTION_KURTOSIS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para evaluar kurtosis",
        )

    kurt = float(s.kurtosis())  # excess kurtosis (fisher=True default)
    abs_kurt = abs(kurt)

    if abs_kurt > 10:
        severity = "HIGH"
    elif abs_kurt > 5:
        severity = "MEDIUM"
    elif abs_kurt > 3:
        severity = "LOW"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="DISTRIBUTION_KURTOSIS", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(kurt, 4), threshold=3.0,
        message=f"Kurtosis excesiva: {kurt:.2f} (colas {'pesadas' if kurt > 0 else 'ligeras'})",
        metadata={"kurtosis": round(kurt, 4)},
    )


def check_negative_values(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """NEGATIVE_VALUES: presencia de negativos."""
    s = _numeric_series(series_typed)
    if len(s) == 0:
        return CheckResult(
            check_id="NEGATIVE_VALUES", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin datos numéricos",
        )

    mask = s < 0
    count = int(mask.sum())
    pct = count / len(s)

    if count == 0:
        severity = "PASS"
    elif pct > 0.20:
        severity = "MEDIUM"
    else:
        severity = "INFO"

    samples = s[mask].head(5).tolist()

    return CheckResult(
        check_id="NEGATIVE_VALUES", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.0,
        message=f"{count:,} valores negativos ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_zero_values(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """ZERO_VALUES: % de ceros (puede indicar valores faltantes codificados)."""
    s = _numeric_series(series_typed)
    if len(s) == 0:
        return CheckResult(
            check_id="ZERO_VALUES", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin datos numéricos",
        )

    mask = s == 0
    count = int(mask.sum())
    pct = count / len(s)

    severity = _severity_from_thresholds(pct, THRESHOLDS_ZERO)

    return CheckResult(
        check_id="ZERO_VALUES", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=THRESHOLDS_ZERO.get(severity, 0.0),
        message=f"{count:,} valores cero ({pct:.1%})",
        affected_count=count, affected_pct=pct,
    )


def check_trend_change(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """TREND_CHANGE: cambio significativo en media móvil vs histórico."""
    s = _numeric_series(series_typed)
    if len(s) < 50:
        return CheckResult(
            check_id="TREND_CHANGE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para análisis de tendencia",
        )

    global_mean = s.mean()
    global_std = s.std()
    if global_std == 0:
        return CheckResult(
            check_id="TREND_CHANGE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Desviación estándar es 0, sin variabilidad",
        )

    # Dividir en ventanas
    n_windows = 5
    window_size = len(s) // n_windows
    windows = [s.iloc[i * window_size:(i + 1) * window_size] for i in range(n_windows)]

    # Comparar última ventana vs media global
    last_window_mean = windows[-1].mean()
    delta_std = abs(last_window_mean - global_mean) / global_std

    severity = _severity_from_thresholds(delta_std, THRESHOLDS_TREND)

    # Mann-Kendall test
    mk_result = {}
    try:
        import pymannkendall as mk
        result = mk.original_test(s.values)
        mk_result = {"trend": result.trend, "p_value": round(result.p, 4), "tau": round(result.Tau, 4)}
    except Exception:
        mk_result = {"trend": "no disponible"}

    return CheckResult(
        check_id="TREND_CHANGE", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(delta_std, 4), threshold=THRESHOLDS_TREND.get(severity, 0.0),
        message=f"Última ventana a {delta_std:.1f}σ de la media global",
        affected_count=len(windows[-1]),
        affected_pct=len(windows[-1]) / len(s),
        metadata={"delta_std": round(delta_std, 4), "mann_kendall": mk_result},
    )


def check_value_range(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """VALUE_RANGE: valores fuera del rango percentil [0.1, 99.9]."""
    s = _numeric_series(series_typed)
    if len(s) < 10:
        return CheckResult(
            check_id="VALUE_RANGE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes",
        )

    p_low, p_high = s.quantile(0.001), s.quantile(0.999)
    mask = (s < p_low) | (s > p_high)
    count = int(mask.sum())
    pct = count / len(s)

    severity = "INFO" if count > 0 else "PASS"
    samples = s[mask].head(5).tolist()

    return CheckResult(
        check_id="VALUE_RANGE", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.0,
        message=f"{count:,} valores fuera del rango [{p_low:.2f}, {p_high:.2f}]",
        affected_count=count, affected_pct=pct, sample_values=samples,
        metadata={"p0.1": round(float(p_low), 4), "p99.9": round(float(p_high), 4)},
    )


def check_variance_sudden_change(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """VARIANCE_SUDDEN_CHANGE: cambio abrupto en varianza entre segmentos."""
    s = _numeric_series(series_typed)
    if len(s) < 100:
        return CheckResult(
            check_id="VARIANCE_SUDDEN_CHANGE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para análisis de varianza",
        )

    mid = len(s) // 2
    var_first = s.iloc[:mid].var()
    var_second = s.iloc[mid:].var()

    if var_first == 0 and var_second == 0:
        ratio = 1.0
    elif var_first == 0 or var_second == 0:
        ratio = float("inf")
    else:
        ratio = max(var_first, var_second) / min(var_first, var_second)

    if ratio > 5.0:
        severity = "HIGH"
    elif ratio > 3.0:
        severity = "MEDIUM"
    elif ratio > 2.0:
        severity = "LOW"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="VARIANCE_SUDDEN_CHANGE", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(ratio, 4), threshold=2.0,
        message=f"Ratio de varianza entre mitades: {ratio:.2f}x",
        metadata={"var_first_half": round(float(var_first), 4), "var_second_half": round(float(var_second), 4)},
    )


def check_normality_test(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """NORMALITY_TEST: Shapiro-Wilk (n<5000) o D'Agostino-K² para normalidad."""
    s = _numeric_series(series_typed)
    if len(s) < 20:
        return CheckResult(
            check_id="NORMALITY_TEST", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.05,
            message="Datos insuficientes para test de normalidad",
        )

    if len(s) < 5000:
        stat, p_value = stats.shapiro(s.sample(n=min(len(s), 5000), random_state=42))
        test_name = "Shapiro-Wilk"
    else:
        stat, p_value = stats.normaltest(s.sample(n=5000, random_state=42))
        test_name = "D'Agostino-K²"

    is_normal = p_value > 0.05
    severity = "PASS" if is_normal else "INFO"

    return CheckResult(
        check_id="NORMALITY_TEST", column=series_raw.name,
        passed=is_normal, severity=severity,
        value=round(p_value, 6), threshold=0.05,
        message=f"{test_name}: p={p_value:.4f} — {'distribución normal' if is_normal else 'distribución no normal'}",
        metadata={"test": test_name, "statistic": round(float(stat), 6), "p_value": round(float(p_value), 6)},
    )


NUMERIC_CHECKS = [
    {"check_id": "OUTLIER_IQR", "function": check_outlier_iqr},
    {"check_id": "OUTLIER_ZSCORE", "function": check_outlier_zscore},
    {"check_id": "OUTLIER_MODIFIED_Z", "function": check_outlier_modified_z},
    {"check_id": "DISTRIBUTION_SKEW", "function": check_distribution_skew},
    {"check_id": "DISTRIBUTION_KURTOSIS", "function": check_distribution_kurtosis},
    {"check_id": "NEGATIVE_VALUES", "function": check_negative_values},
    {"check_id": "ZERO_VALUES", "function": check_zero_values},
    {"check_id": "TREND_CHANGE", "function": check_trend_change},
    {"check_id": "VALUE_RANGE", "function": check_value_range},
    {"check_id": "VARIANCE_SUDDEN_CHANGE", "function": check_variance_sudden_change},
    {"check_id": "NORMALITY_TEST", "function": check_normality_test},
]
