"""
Temporal Completeness Analysis — analiza tasa de nulos por periodo cuando hay columna de fecha.
Detecta ventanas donde la captura de datos se degradó.
"""

from typing import List, Optional, Dict

import pandas as pd
import numpy as np

from models.check_result import CheckResult
from models.semantic_type import SemanticType


def run_temporal_completeness_checks(
    df: pd.DataFrame,
    df_raw: pd.DataFrame,
    column_types: Dict[str, SemanticType],
    date_col: Optional[str] = None,
) -> List[CheckResult]:
    """Analiza completitud de datos por periodo temporal."""
    results = []

    # Encontrar columna de fecha
    dt_col = date_col
    if not dt_col:
        for col, stype in column_types.items():
            if stype in (SemanticType.DATE, SemanticType.DATETIME):
                dt_col = col
                break

    if not dt_col or dt_col not in df.columns:
        return results

    dt = pd.to_datetime(df[dt_col], errors="coerce")
    valid_mask = dt.notna()
    if valid_mask.sum() < 10:
        return results

    # Crear un DataFrame con la fecha como índice
    df_with_date = df.copy()
    df_with_date["__date__"] = dt
    df_with_date = df_with_date[valid_mask]

    if len(df_with_date) < 10:
        return results

    # Determinar frecuencia óptima
    date_range = (dt.max() - dt.min()).days
    if date_range > 365:
        freq = "ME"
        freq_label = "mes"
    elif date_range > 30:
        freq = "W"
        freq_label = "semana"
    else:
        freq = "D"
        freq_label = "día"

    # Análisis por periodo: tasa de nulos de cada columna
    numeric_cols = [c for c, t in column_types.items()
                    if t in (SemanticType.NUMERIC_CONTINUOUS, SemanticType.NUMERIC_DISCRETE)
                    and c in df.columns and c != dt_col]

    all_cols = [c for c in df.columns if c != dt_col and c in column_types]

    if not all_cols:
        return results

    # Calcular tasa de nulos global por periodo
    df_with_date = df_with_date.set_index("__date__")

    try:
        period_null_rates = df_with_date[all_cols].resample(freq).apply(
            lambda x: x.isna().mean().mean()  # promedio de tasa nulos de todas las cols
        )
    except Exception:
        return results

    if len(period_null_rates) < 3:
        return results

    # Detectar periodos con degradación (null rate > 2x el promedio)
    avg_null_rate = period_null_rates.mean()
    if avg_null_rate > 0:
        degraded = period_null_rates[period_null_rates > avg_null_rate * 2]

        if len(degraded) > 0:
            worst_period = degraded.idxmax()
            worst_rate = float(degraded.max())

            results.append(CheckResult(
                check_id="TEMPORAL_COMPLETENESS",
                column="__dataset__",
                passed=False,
                severity="HIGH" if worst_rate > 0.5 else "MEDIUM",
                value=round(worst_rate, 4),
                threshold=round(avg_null_rate * 2, 4),
                message=f"Degradación de completitud temporal: {len(degraded)} {freq_label}(s) "
                        f"con tasa de nulos > 2x promedio — peor periodo: "
                        f"{str(worst_period)[:10]} ({worst_rate:.1%} nulos vs {avg_null_rate:.1%} promedio)",
                affected_count=len(degraded),
                affected_pct=len(degraded) / len(period_null_rates),
                metadata={
                    "degraded_periods": [str(d)[:10] for d in degraded.index[:10]],
                    "avg_null_rate": round(avg_null_rate, 4),
                    "worst_period": str(worst_period)[:10],
                    "worst_null_rate": round(worst_rate, 4),
                    "frequency": freq_label,
                },
            ))

    # Análisis por columna: detectar columnas con nulidad concentrada temporalmente
    for col in all_cols[:20]:  # limitar a 20 columnas
        col_null_pct = float(df[col].isna().mean())
        if col_null_pct < 0.01 or col_null_pct > 0.95:
            continue

        try:
            col_null_by_period = df_with_date[col].resample(freq).apply(
                lambda x: x.isna().mean()
            )
        except Exception:
            continue

        if len(col_null_by_period) < 3:
            continue

        # Detectar si los nulos están concentrados en pocos periodos
        high_null_periods = col_null_by_period[col_null_by_period > col_null_pct * 3]
        if len(high_null_periods) > 0 and len(high_null_periods) <= len(col_null_by_period) * 0.3:
            results.append(CheckResult(
                check_id="TEMPORAL_NULL_CONCENTRATION",
                column=col,
                passed=False,
                severity="MEDIUM",
                value=round(float(high_null_periods.max()), 4),
                threshold=round(col_null_pct * 3, 4),
                message=f"Nulos concentrados temporalmente: {len(high_null_periods)} {freq_label}(s) "
                        f"con >3x la tasa promedio de nulos ({col_null_pct:.1%})",
                affected_count=len(high_null_periods),
                affected_pct=len(high_null_periods) / len(col_null_by_period),
                metadata={
                    "high_null_periods": [str(d)[:10] for d in high_null_periods.index[:5]],
                    "avg_null_rate": round(col_null_pct, 4),
                },
            ))

    return results
