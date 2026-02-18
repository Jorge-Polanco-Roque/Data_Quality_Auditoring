"""
Análisis de patrones de nulidad: MCAR/MAR/MNAR, correlaciones de nulos, patrones de filas.

Estos checks operan a nivel dataset.
"""

import numpy as np
import pandas as pd
from scipy import stats

from models.check_result import CheckResult


def run_null_pattern_checks(df, df_raw):
    """Ejecuta checks de patrones de nulidad. Retorna lista de CheckResult."""
    results = []
    results.extend(_null_correlation(df))
    results.extend(_null_row_patterns(df))
    results.extend(_littles_mcar_approx(df))
    return results


# ---------------------------------------------------------------------------
# 1. Null correlation matrix — ¿los nulos se concentran en las mismas filas?
# ---------------------------------------------------------------------------

def _null_correlation(df):
    results = []
    null_matrix = df.isnull().astype(int)

    # Solo columnas con al menos 1% de nulos
    null_pcts = null_matrix.mean()
    cols_with_nulls = null_pcts[null_pcts > 0.01].index.tolist()
    if len(cols_with_nulls) < 2:
        return results

    corr_pairs = []
    for i, col_a in enumerate(cols_with_nulls):
        for col_b in cols_with_nulls[i + 1:]:
            r, p = stats.pearsonr(null_matrix[col_a], null_matrix[col_b])
            if abs(r) > 0.5 and p < 0.05:
                corr_pairs.append({
                    "pair": f"{col_a} × {col_b}",
                    "correlation": round(float(r), 4),
                    "p_value": round(float(p), 6),
                })

    if corr_pairs:
        samples = [f"{p['pair']}: r={p['correlation']}" for p in corr_pairs[:5]]
        results.append(CheckResult(
            check_id="NULL_CORRELATION", column="__dataset__",
            passed=False, severity="MEDIUM",
            value=float(len(corr_pairs)), threshold=0.5,
            message=f"{len(corr_pairs)} par(es) de columnas con nulos correlacionados (posible MAR/MNAR)",
            sample_values=samples,
            metadata={"correlated_pairs": corr_pairs},
        ))
    return results


# ---------------------------------------------------------------------------
# 2. Null row patterns — filas con muchos nulos simultáneos
# ---------------------------------------------------------------------------

def _null_row_patterns(df):
    results = []
    n_cols = len(df.columns)
    if n_cols < 3:
        return results

    null_per_row = df.isnull().sum(axis=1)
    # Filas con >50% nulos
    multi_null = null_per_row > (n_cols * 0.5)
    count = int(multi_null.sum())
    pct = count / len(df) if len(df) > 0 else 0.0

    if count == 0:
        return results

    if pct > 0.10:
        severity = "HIGH"
    elif pct > 0.05:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    # Patrón más común de nulls
    null_patterns = df.isnull().apply(lambda row: tuple(row), axis=1)
    pattern_counts = null_patterns.value_counts()
    top_patterns = []
    for pattern, cnt in pattern_counts.head(3).items():
        if sum(pattern) > 0:
            cols_null = [df.columns[i] for i, v in enumerate(pattern) if v]
            top_patterns.append({"cols_null": cols_null, "count": int(cnt)})

    results.append(CheckResult(
        check_id="NULL_ROW_PATTERN", column="__dataset__",
        passed=False, severity=severity,
        value=round(pct, 4), threshold=0.05,
        message=f"{count:,} filas con >50% de columnas nulas ({pct:.1%})",
        affected_count=count, affected_pct=pct,
        sample_values=[f"Patrón: {p['cols_null'][:5]} ({p['count']} filas)" for p in top_patterns[:3]],
        metadata={"top_patterns": top_patterns},
    ))
    return results


# ---------------------------------------------------------------------------
# 3. Little's MCAR test (aproximación simplificada)
# ---------------------------------------------------------------------------

def _littles_mcar_approx(df):
    """Aproximación al test MCAR de Little usando chi-squared sobre patrones de nulidad."""
    results = []

    # Solo columnas numéricas con nulos
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cols_with_nulls = [c for c in num_cols if df[c].isnull().any()]
    if len(cols_with_nulls) < 1 or len(df) < 30:
        return results

    # Simplificación: comparar medias de columnas observadas cuando otra es nula vs no nula
    mcar_violations = []
    for null_col in cols_with_nulls[:5]:
        mask = df[null_col].isnull()
        n_null = mask.sum()
        n_obs = (~mask).sum()
        if n_null < 5 or n_obs < 5:
            continue

        for other_col in num_cols:
            if other_col == null_col:
                continue
            obs_when_null = pd.to_numeric(df.loc[mask, other_col], errors="coerce").dropna()
            obs_when_present = pd.to_numeric(df.loc[~mask, other_col], errors="coerce").dropna()
            if len(obs_when_null) < 5 or len(obs_when_present) < 5:
                continue

            try:
                stat, p = stats.mannwhitneyu(obs_when_null, obs_when_present, alternative="two-sided")
                if p < 0.01:
                    mcar_violations.append({
                        "null_col": null_col,
                        "tested_col": other_col,
                        "p_value": round(float(p), 6),
                    })
            except Exception:
                continue

    if mcar_violations:
        severity = "HIGH" if len(mcar_violations) > 3 else "MEDIUM"
        samples = [f"Nulls en {v['null_col']} afectan {v['tested_col']} (p={v['p_value']})"
                   for v in mcar_violations[:5]]
        results.append(CheckResult(
            check_id="MCAR_VIOLATION", column="__dataset__",
            passed=False, severity=severity,
            value=float(len(mcar_violations)), threshold=0.01,
            message=f"{len(mcar_violations)} violación(es) MCAR: nulos NO son completamente aleatorios "
                    f"(posible MAR/MNAR)",
            sample_values=samples,
            metadata={"violations": mcar_violations},
        ))
    return results
