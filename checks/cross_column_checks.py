"""
Análisis cross-column: correlaciones, multicolinealidad y asociaciones.

Estos checks operan a nivel dataset (no por columna individual).
"""

import numpy as np
import pandas as pd
from scipy import stats

from models.check_result import CheckResult
from models.semantic_type import SemanticType


def run_cross_column_checks(df, df_raw, column_types):
    """Ejecuta todos los checks cross-column. Retorna lista de CheckResult."""
    results = []
    results.extend(_correlation_matrix(df, column_types))
    results.extend(_vif_check(df, column_types))
    results.extend(_cramers_v_matrix(df, column_types))
    results.extend(_point_biserial(df, column_types))
    return results


def _get_numeric_cols(df, column_types):
    return [c for c, t in column_types.items()
            if t in (SemanticType.NUMERIC_CONTINUOUS, SemanticType.NUMERIC_DISCRETE)
            and c in df.columns]


def _get_categorical_cols(df, column_types):
    return [c for c, t in column_types.items()
            if t in (SemanticType.CATEGORICAL, SemanticType.BOOLEAN)
            and c in df.columns]


# ---------------------------------------------------------------------------
# 1. Correlation matrix (Pearson + Spearman)
# ---------------------------------------------------------------------------

def _correlation_matrix(df, column_types):
    results = []
    num_cols = _get_numeric_cols(df, column_types)
    if len(num_cols) < 2:
        return results

    num_df = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # Pearson
    try:
        pearson = num_df.corr(method="pearson")
    except Exception:
        return results

    # Spearman
    try:
        spearman = num_df.corr(method="spearman")
    except Exception:
        spearman = None

    high_corr_pairs = []
    for i, col_a in enumerate(num_cols):
        for col_b in num_cols[i + 1:]:
            r = pearson.loc[col_a, col_b]
            if abs(r) > 0.85:
                rho = spearman.loc[col_a, col_b] if spearman is not None else None
                high_corr_pairs.append({
                    "pair": f"{col_a} × {col_b}",
                    "pearson_r": round(float(r), 4),
                    "spearman_rho": round(float(rho), 4) if rho is not None else None,
                })

    if high_corr_pairs:
        severity = "HIGH" if any(abs(p["pearson_r"]) > 0.95 for p in high_corr_pairs) else "MEDIUM"
        samples = [f"{p['pair']}: r={p['pearson_r']}" for p in high_corr_pairs[:5]]
        results.append(CheckResult(
            check_id="HIGH_CORRELATION", column="__dataset__",
            passed=False, severity=severity,
            value=float(len(high_corr_pairs)), threshold=0.85,
            message=f"{len(high_corr_pairs)} par(es) con correlación |r| > 0.85",
            sample_values=samples,
            metadata={"pairs": high_corr_pairs,
                      "pearson_matrix": {c: {c2: round(float(pearson.loc[c, c2]), 4)
                                             for c2 in num_cols} for c in num_cols}},
        ))
    return results


# ---------------------------------------------------------------------------
# 2. VIF (Variance Inflation Factor) para multicolinealidad
# ---------------------------------------------------------------------------

def _vif_check(df, column_types):
    results = []
    num_cols = _get_numeric_cols(df, column_types)
    if len(num_cols) < 2:
        return results

    num_df = df[num_cols].apply(pd.to_numeric, errors="coerce").dropna()
    if len(num_df) < 10 or len(num_cols) > 50:
        return results

    # Calcular VIF para cada variable
    vif_values = {}
    X = num_df.values
    for i, col in enumerate(num_cols):
        try:
            # VIF = 1 / (1 - R²) donde R² viene de regresar col_i contra el resto
            y = X[:, i]
            X_others = np.delete(X, i, axis=1)
            if X_others.shape[1] == 0:
                continue
            # Añadir intercepto
            X_aug = np.column_stack([np.ones(len(X_others)), X_others])
            # Resolver por OLS
            try:
                coeffs, residuals, rank, sv = np.linalg.lstsq(X_aug, y, rcond=None)
                y_hat = X_aug @ coeffs
                ss_res = np.sum((y - y_hat) ** 2)
                ss_tot = np.sum((y - y.mean()) ** 2)
                r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
                vif = 1.0 / (1.0 - r_squared) if r_squared < 1.0 else float("inf")
                vif_values[col] = round(float(vif), 2)
            except Exception:
                continue
        except Exception:
            continue

    high_vif = {k: v for k, v in vif_values.items() if v > 5.0}
    if high_vif:
        severity = "HIGH" if any(v > 10 for v in high_vif.values()) else "MEDIUM"
        samples = [f"{col}: VIF={vif}" for col, vif in sorted(high_vif.items(), key=lambda x: -x[1])[:5]]
        results.append(CheckResult(
            check_id="MULTICOLLINEARITY_VIF", column="__dataset__",
            passed=False, severity=severity,
            value=float(max(high_vif.values())), threshold=5.0,
            message=f"{len(high_vif)} columna(s) con VIF > 5 (multicolinealidad)",
            sample_values=samples,
            metadata={"vif_values": vif_values},
        ))
    return results


# ---------------------------------------------------------------------------
# 3. Cramér's V matrix entre categóricas
# ---------------------------------------------------------------------------

def _cramers_v_matrix(df, column_types):
    results = []
    cat_cols = _get_categorical_cols(df, column_types)
    if len(cat_cols) < 2:
        return results

    strong_pairs = []
    for i, col_a in enumerate(cat_cols):
        for col_b in cat_cols[i + 1:]:
            try:
                contingency = pd.crosstab(df[col_a].fillna("__NULL__"), df[col_b].fillna("__NULL__"))
                if contingency.shape[0] < 2 or contingency.shape[1] < 2:
                    continue
                chi2, p, dof, expected = stats.chi2_contingency(contingency)
                n = contingency.sum().sum()
                min_dim = min(contingency.shape) - 1
                v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 and n > 0 else 0
                if v > 0.5 and p < 0.05:
                    strong_pairs.append({
                        "pair": f"{col_a} × {col_b}",
                        "cramers_v": round(float(v), 4),
                        "p_value": round(float(p), 6),
                    })
            except Exception:
                continue

    if strong_pairs:
        samples = [f"{p['pair']}: V={p['cramers_v']}" for p in strong_pairs[:5]]
        results.append(CheckResult(
            check_id="CATEGORICAL_ASSOCIATION", column="__dataset__",
            passed=False, severity="MEDIUM",
            value=float(len(strong_pairs)), threshold=0.5,
            message=f"{len(strong_pairs)} par(es) de categóricas con Cramér's V > 0.5",
            sample_values=samples,
            metadata={"pairs": strong_pairs},
        ))
    return results


# ---------------------------------------------------------------------------
# 4. Point-biserial correlation (numérica vs binaria)
# ---------------------------------------------------------------------------

def _point_biserial(df, column_types):
    results = []
    num_cols = _get_numeric_cols(df, column_types)
    bool_cols = [c for c, t in column_types.items()
                 if t == SemanticType.BOOLEAN and c in df.columns]

    if not num_cols or not bool_cols:
        return results

    strong_assocs = []
    for bool_col in bool_cols:
        # Convertir a 0/1
        binary = df[bool_col].astype(str).str.strip().str.lower()
        binary_map = binary.map(lambda x: 1 if x in ("true", "t", "yes", "y", "si", "sí", "1", "verdadero") else
                                          (0 if x in ("false", "f", "no", "n", "0", "falso") else np.nan))

        for num_col in num_cols:
            try:
                numeric = pd.to_numeric(df[num_col], errors="coerce")
                valid = pd.DataFrame({"num": numeric, "bin": binary_map}).dropna()
                if len(valid) < 10 or valid["bin"].nunique() < 2:
                    continue
                r, p = stats.pointbiserialr(valid["bin"], valid["num"])
                if abs(r) > 0.5 and p < 0.05:
                    strong_assocs.append({
                        "pair": f"{num_col} × {bool_col}",
                        "r_pb": round(float(r), 4),
                        "p_value": round(float(p), 6),
                    })
            except Exception:
                continue

    if strong_assocs:
        samples = [f"{a['pair']}: r_pb={a['r_pb']}" for a in strong_assocs[:5]]
        results.append(CheckResult(
            check_id="POINT_BISERIAL", column="__dataset__",
            passed=True, severity="INFO",
            value=float(len(strong_assocs)), threshold=0.5,
            message=f"{len(strong_assocs)} asociación(es) punto-biserial significativa(s) (|r| > 0.5)",
            sample_values=samples,
            metadata={"associations": strong_assocs},
        ))
    return results
