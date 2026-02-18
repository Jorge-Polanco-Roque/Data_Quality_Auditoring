"""
Pruebas de hipótesis paramétricas y no paramétricas.

Flujo adaptativo:
  1. Gate de normalidad (Shapiro-Wilk / D'Agostino / Anderson-Darling / Lilliefors)
  2. Si normal → ruta paramétrica (t-test, Bartlett)
  3. Si no normal → ruta no paramétrica (Mann-Whitney, Levene)
  4. Tests de distribución, asociación categórica y estacionariedad
"""

import warnings

import numpy as np
import pandas as pd
from scipy import stats

from models.check_result import CheckResult


MIN_SAMPLE = 20  # mínimo para tests estadísticos
ALPHA = 0.05


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _numeric(series_typed: pd.Series) -> pd.Series:
    return pd.to_numeric(series_typed, errors="coerce").dropna()


def _split_halves(s: pd.Series):
    """Divide una serie en dos mitades temporales."""
    mid = len(s) // 2
    return s.iloc[:mid], s.iloc[mid:]


def _is_normal(s: pd.Series) -> bool:
    """Gate rápido de normalidad: True si p > 0.05."""
    if len(s) < 8:
        return False
    try:
        if len(s) < 5000:
            _, p = stats.shapiro(s.sample(n=min(len(s), 5000), random_state=42))
        else:
            _, p = stats.normaltest(s.sample(n=5000, random_state=42))
        return p > ALPHA
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 1. NORMALITY TESTS
# ---------------------------------------------------------------------------

def check_normality_anderson(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """NORMALITY_ANDERSON: Anderson-Darling test — más potente que Shapiro para colas."""
    s = _numeric(series_typed)
    if len(s) < MIN_SAMPLE:
        return CheckResult(
            check_id="NORMALITY_ANDERSON", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para Anderson-Darling",
        )

    # Compatible con scipy <1.17 (sin method/pvalue) y >=1.17
    try:
        result = stats.anderson(s, dist="norm", method="interpolate")
        p_value = float(result.pvalue)
    except TypeError:
        result = stats.anderson(s, dist="norm")
        # scipy antiguo: comparar statistic vs critical_values al 5%
        # critical_values[2] corresponde al nivel de significancia 5%
        p_value = 0.10 if result.statistic < result.critical_values[2] else 0.01
    statistic = float(result.statistic)
    is_normal = p_value >= 0.05

    return CheckResult(
        check_id="NORMALITY_ANDERSON", column=series_raw.name,
        passed=is_normal, severity="PASS" if is_normal else "INFO",
        value=round(statistic, 6), threshold=0.05,
        message=f"Anderson-Darling: stat={statistic:.4f}, p={p_value:.4f} "
                f"— {'normal' if is_normal else 'no normal'}",
        metadata={"statistic": round(statistic, 6),
                  "p_value": round(p_value, 6), "is_normal": is_normal},
    )


def check_normality_lilliefors(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """NORMALITY_LILLIEFORS: KS test contra distribución normal con parámetros estimados."""
    s = _numeric(series_typed)
    if len(s) < MIN_SAMPLE:
        return CheckResult(
            check_id="NORMALITY_LILLIEFORS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Datos insuficientes para Lilliefors",
        )

    try:
        from statsmodels.stats.diagnostic import lilliefors as _lilliefors
        stat, p_value = _lilliefors(s, dist="norm")
    except ImportError:
        # Fallback: KS 1-muestra estándar (menos potente que Lilliefors)
        stat, p_value = stats.kstest(s, "norm", args=(s.mean(), s.std()))

    is_normal = p_value > ALPHA

    return CheckResult(
        check_id="NORMALITY_LILLIEFORS", column=series_raw.name,
        passed=is_normal, severity="PASS" if is_normal else "INFO",
        value=round(float(p_value), 6), threshold=ALPHA,
        message=f"Lilliefors: p={p_value:.4f} — {'normal' if is_normal else 'no normal'}",
        metadata={"statistic": round(float(stat), 6), "p_value": round(float(p_value), 6),
                  "is_normal": is_normal},
    )


# ---------------------------------------------------------------------------
# 2. MEAN / LOCATION COMPARISON (adaptive: parametric vs non-parametric)
# ---------------------------------------------------------------------------

def check_mean_comparison(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """MEAN_SHIFT: compara media entre primera y segunda mitad.
    Ruta paramétrica (t-test) si ambas mitades son normales, sino Mann-Whitney U."""
    s = _numeric(series_typed)
    if len(s) < MIN_SAMPLE * 2:
        return CheckResult(
            check_id="MEAN_SHIFT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Datos insuficientes para comparación de medias",
        )

    h1, h2 = _split_halves(s)

    if _is_normal(h1) and _is_normal(h2):
        # Ruta paramétrica: t-test independiente
        stat, p_value = stats.ttest_ind(h1, h2, equal_var=False)  # Welch's t-test
        test_name = "Welch t-test (paramétrico)"
    else:
        # Ruta no paramétrica: Mann-Whitney U
        stat, p_value = stats.mannwhitneyu(h1, h2, alternative="two-sided")
        test_name = "Mann-Whitney U (no paramétrico)"

    is_significant = p_value < ALPHA
    diff_pct = abs(h1.mean() - h2.mean()) / h1.mean() * 100 if h1.mean() != 0 else 0

    if is_significant and diff_pct > 20:
        severity = "HIGH"
    elif is_significant:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="MEAN_SHIFT", column=series_raw.name,
        passed=not is_significant, severity=severity,
        value=round(float(p_value), 6), threshold=ALPHA,
        message=f"{test_name}: p={p_value:.4f} — media 1ra mitad={h1.mean():.2f}, "
                f"2da mitad={h2.mean():.2f} (Δ{diff_pct:.1f}%)"
                f"{' — cambio significativo' if is_significant else ''}",
        metadata={"test": test_name, "statistic": round(float(stat), 6),
                  "p_value": round(float(p_value), 6), "mean_h1": round(float(h1.mean()), 4),
                  "mean_h2": round(float(h2.mean()), 4), "diff_pct": round(diff_pct, 2),
                  "significant": is_significant},
    )


def check_wilcoxon_paired(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """WILCOXON_PAIRED: comparación pareada no paramétrica entre segmentos de igual tamaño."""
    s = _numeric(series_typed)
    if len(s) < MIN_SAMPLE * 2:
        return CheckResult(
            check_id="WILCOXON_PAIRED", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Datos insuficientes para Wilcoxon pareado",
        )

    h1, h2 = _split_halves(s)
    # Igualar tamaños para pareado
    min_len = min(len(h1), len(h2))
    h1, h2 = h1.iloc[:min_len].values, h2.iloc[:min_len].values

    diffs = h1 - h2
    if np.all(diffs == 0):
        return CheckResult(
            check_id="WILCOXON_PAIRED", column=series_raw.name, passed=True,
            severity="PASS", value=1.0, threshold=ALPHA,
            message="Sin diferencias entre segmentos pareados",
        )

    stat, p_value = stats.wilcoxon(h1, h2)
    is_significant = p_value < ALPHA

    severity = "MEDIUM" if is_significant else "PASS"

    return CheckResult(
        check_id="WILCOXON_PAIRED", column=series_raw.name,
        passed=not is_significant, severity=severity,
        value=round(float(p_value), 6), threshold=ALPHA,
        message=f"Wilcoxon signed-rank: p={p_value:.4f}"
                f"{' — diferencia pareada significativa' if is_significant else ' — sin diferencia significativa'}",
        metadata={"test": "Wilcoxon signed-rank", "statistic": round(float(stat), 6),
                  "p_value": round(float(p_value), 6), "significant": is_significant},
    )


# ---------------------------------------------------------------------------
# 3. VARIANCE COMPARISON (adaptive)
# ---------------------------------------------------------------------------

def check_variance_comparison(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """VARIANCE_SHIFT: compara varianza entre mitades.
    Bartlett (paramétrico) si ambas normales, Levene (no paramétrico) siempre como robusto."""
    s = _numeric(series_typed)
    if len(s) < MIN_SAMPLE * 2:
        return CheckResult(
            check_id="VARIANCE_SHIFT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Datos insuficientes para comparación de varianzas",
        )

    h1, h2 = _split_halves(s)
    results_meta = {}

    if _is_normal(h1) and _is_normal(h2):
        stat, p_value = stats.bartlett(h1, h2)
        test_name = "Bartlett (paramétrico)"
        results_meta["bartlett_stat"] = round(float(stat), 6)
        results_meta["bartlett_p"] = round(float(p_value), 6)
    else:
        stat, p_value = stats.levene(h1, h2, center="median")
        test_name = "Levene (no paramétrico)"
        results_meta["levene_stat"] = round(float(stat), 6)
        results_meta["levene_p"] = round(float(p_value), 6)

    is_significant = p_value < ALPHA
    var_ratio = float(h1.var() / h2.var()) if h2.var() > 0 else float("inf")

    if is_significant and (var_ratio > 3 or var_ratio < 1/3):
        severity = "HIGH"
    elif is_significant:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    results_meta.update({
        "test": test_name, "p_value": round(float(p_value), 6),
        "var_h1": round(float(h1.var()), 4), "var_h2": round(float(h2.var()), 4),
        "var_ratio": round(var_ratio, 4), "significant": is_significant,
    })

    return CheckResult(
        check_id="VARIANCE_SHIFT", column=series_raw.name,
        passed=not is_significant, severity=severity,
        value=round(float(p_value), 6), threshold=ALPHA,
        message=f"{test_name}: p={p_value:.4f} — var 1ra={h1.var():.2f}, 2da={h2.var():.2f} "
                f"(ratio={var_ratio:.2f}x)"
                f"{' — cambio significativo' if is_significant else ''}",
        metadata=results_meta,
    )


# ---------------------------------------------------------------------------
# 4. DISTRIBUTION COMPARISON
# ---------------------------------------------------------------------------

def check_ks_goodness_of_fit(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """KS_GOODNESS_FIT: KS 1-muestra — ¿los datos siguen una distribución normal?"""
    s = _numeric(series_typed)
    if len(s) < MIN_SAMPLE:
        return CheckResult(
            check_id="KS_GOODNESS_FIT", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Datos insuficientes para KS goodness-of-fit",
        )

    stat, p_value = stats.kstest(s, "norm", args=(s.mean(), s.std()))
    fits_normal = p_value > ALPHA

    return CheckResult(
        check_id="KS_GOODNESS_FIT", column=series_raw.name,
        passed=fits_normal, severity="PASS" if fits_normal else "INFO",
        value=round(float(p_value), 6), threshold=ALPHA,
        message=f"KS goodness-of-fit (normal): p={p_value:.4f} "
                f"— {'compatible con normal' if fits_normal else 'no compatible con normal'}",
        metadata={"statistic": round(float(stat), 6), "p_value": round(float(p_value), 6),
                  "fits_normal": fits_normal},
    )


# ---------------------------------------------------------------------------
# 5. CATEGORICAL ASSOCIATION
# ---------------------------------------------------------------------------

def check_chi2_independence(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """CHI2_INDEPENDENCE: test Chi² entre esta columna categórica y otras categóricas del dataset."""
    df = metadata.get("_df")
    if df is None:
        return CheckResult(
            check_id="CHI2_INDEPENDENCE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Sin acceso al DataFrame completo",
        )

    col_name = series_raw.name
    cat_cols = [c for c in df.columns if c != col_name and df[c].dtype == "object"
                and df[c].nunique() < 50 and df[c].nunique() > 1]

    if not cat_cols:
        return CheckResult(
            check_id="CHI2_INDEPENDENCE", column=col_name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="No hay otras columnas categóricas para test de independencia",
        )

    associations = []
    for other_col in cat_cols[:5]:  # Limitar a 5 pares
        contingency = pd.crosstab(df[col_name].fillna("__NULL__"), df[other_col].fillna("__NULL__"))
        if contingency.shape[0] < 2 or contingency.shape[1] < 2:
            continue
        chi2, p_value, dof, expected = stats.chi2_contingency(contingency)

        # Cramér's V
        n = contingency.sum().sum()
        min_dim = min(contingency.shape) - 1
        cramers_v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 and n > 0 else 0

        associations.append({
            "column_pair": f"{col_name} × {other_col}",
            "chi2": round(float(chi2), 4),
            "p_value": round(float(p_value), 6),
            "dof": int(dof),
            "cramers_v": round(float(cramers_v), 4),
            "significant": p_value < ALPHA,
        })

    significant_assocs = [a for a in associations if a["significant"]]
    strong_assocs = [a for a in significant_assocs if a["cramers_v"] > 0.3]

    if strong_assocs:
        severity = "MEDIUM"
    elif significant_assocs:
        severity = "LOW"
    else:
        severity = "PASS"

    samples = [f"{a['column_pair']}: V={a['cramers_v']}, p={a['p_value']}" for a in associations[:5]]

    return CheckResult(
        check_id="CHI2_INDEPENDENCE", column=col_name,
        passed=severity == "PASS", severity=severity,
        value=float(len(significant_assocs)), threshold=ALPHA,
        message=f"{len(significant_assocs)} asociación(es) significativa(s) de {len(associations)} pares evaluados"
                f"{f' ({len(strong_assocs)} fuerte(s) con V>0.3)' if strong_assocs else ''}",
        sample_values=samples,
        metadata={"associations": associations},
    )


def check_kruskal_wallis(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """KRUSKAL_WALLIS: ¿una columna numérica difiere entre grupos de una categórica?"""
    df = metadata.get("_df")
    if df is None:
        return CheckResult(
            check_id="KRUSKAL_WALLIS", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Sin acceso al DataFrame completo",
        )

    col_name = series_raw.name
    s = _numeric(series_typed)
    if len(s) < MIN_SAMPLE:
        return CheckResult(
            check_id="KRUSKAL_WALLIS", column=col_name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Datos insuficientes",
        )

    # Buscar columnas categóricas con 2-20 grupos
    cat_cols = [c for c in df.columns if c != col_name and df[c].dtype == "object"
                and 2 <= df[c].nunique() <= 20]

    if not cat_cols:
        return CheckResult(
            check_id="KRUSKAL_WALLIS", column=col_name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="No hay columnas categóricas con 2-20 grupos para Kruskal-Wallis",
        )

    results_list = []
    for cat_col in cat_cols[:3]:  # Limitar a 3
        groups = []
        for name, group in df.groupby(cat_col)[col_name]:
            g = pd.to_numeric(group, errors="coerce").dropna()
            if len(g) >= 5:
                groups.append(g.values)

        if len(groups) < 2:
            continue

        stat, p_value = stats.kruskal(*groups)
        results_list.append({
            "grouping_col": cat_col,
            "statistic": round(float(stat), 4),
            "p_value": round(float(p_value), 6),
            "n_groups": len(groups),
            "significant": p_value < ALPHA,
        })

    significant = [r for r in results_list if r["significant"]]

    if not results_list:
        severity = "PASS"
    elif significant:
        severity = "INFO"
    else:
        severity = "PASS"

    samples = [f"{r['grouping_col']}: H={r['statistic']}, p={r['p_value']}, "
               f"{'SIG' if r['significant'] else 'NS'}" for r in results_list]

    return CheckResult(
        check_id="KRUSKAL_WALLIS", column=col_name,
        passed=True, severity=severity,
        value=float(len(significant)), threshold=ALPHA,
        message=f"Kruskal-Wallis: {len(significant)} agrupación(es) significativa(s) de {len(results_list)} evaluadas",
        sample_values=samples,
        metadata={"results": results_list},
    )


# ---------------------------------------------------------------------------
# 6. STATIONARITY
# ---------------------------------------------------------------------------

def check_stationarity_adf(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """ADF_STATIONARITY: Augmented Dickey-Fuller — ¿la serie temporal es estacionaria?"""
    s = _numeric(series_typed)
    if len(s) < 30:
        return CheckResult(
            check_id="ADF_STATIONARITY", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=ALPHA,
            message="Datos insuficientes para test ADF",
        )

    try:
        from statsmodels.tsa.stattools import adfuller
        result = adfuller(s, autolag="AIC")
        adf_stat, p_value, used_lag, nobs, critical_values, icbest = result
    except ImportError:
        return CheckResult(
            check_id="ADF_STATIONARITY", column=series_raw.name, passed=True,
            severity="INFO", value=0.0, threshold=ALPHA,
            message="statsmodels no disponible para test ADF",
        )

    is_stationary = p_value < ALPHA  # H0: tiene raíz unitaria (no estacionaria)

    if not is_stationary and p_value > 0.10:
        severity = "MEDIUM"
    elif not is_stationary:
        severity = "LOW"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="ADF_STATIONARITY", column=series_raw.name,
        passed=is_stationary, severity=severity,
        value=round(float(p_value), 6), threshold=ALPHA,
        message=f"ADF: stat={adf_stat:.4f}, p={p_value:.4f} "
                f"— {'estacionaria' if is_stationary else 'no estacionaria (posible tendencia o raíz unitaria)'}",
        metadata={
            "adf_statistic": round(float(adf_stat), 6),
            "p_value": round(float(p_value), 6),
            "used_lag": int(used_lag),
            "critical_values": {k: round(float(v), 4) for k, v in critical_values.items()},
            "is_stationary": is_stationary,
        },
    )


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

# Tests para columnas numéricas
HYPOTHESIS_NUMERIC_CHECKS = [
    {"check_id": "NORMALITY_ANDERSON", "function": check_normality_anderson},
    {"check_id": "NORMALITY_LILLIEFORS", "function": check_normality_lilliefors},
    {"check_id": "MEAN_SHIFT", "function": check_mean_comparison},
    {"check_id": "WILCOXON_PAIRED", "function": check_wilcoxon_paired},
    {"check_id": "VARIANCE_SHIFT", "function": check_variance_comparison},
    {"check_id": "KS_GOODNESS_FIT", "function": check_ks_goodness_of_fit},
    {"check_id": "ADF_STATIONARITY", "function": check_stationarity_adf},
]

# Tests para columnas categóricas
HYPOTHESIS_CATEGORICAL_CHECKS = [
    {"check_id": "CHI2_INDEPENDENCE", "function": check_chi2_independence},
    {"check_id": "KRUSKAL_WALLIS", "function": check_kruskal_wallis},
]
