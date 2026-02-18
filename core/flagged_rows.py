"""
Flagged Rows Export — genera un CSV con las filas problemáticas y sus motivos.
"""

import os
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

from models.check_result import CheckResult
from models.semantic_type import SemanticType


class FlaggedRowsExporter:
    """Identifica y exporta filas problemáticas a un CSV auxiliar."""

    def collect_flagged_rows(
        self,
        df_raw: pd.DataFrame,
        df: pd.DataFrame,
        results: List[CheckResult],
        column_types: Dict[str, SemanticType],
    ) -> pd.DataFrame:
        """Recolecta filas con problemas detectados. Retorna DataFrame de flags."""
        flags = []  # list of dicts: {row, column, check_id, severity, detail}

        n_rows = len(df)

        for r in results:
            if r.passed or r.column == "__dataset__":
                continue

            col = r.column
            if col not in df.columns:
                continue

            row_indices = self._get_flagged_indices(r, df_raw, df, col)

            for idx in row_indices:
                if 0 <= idx < n_rows:
                    flags.append({
                        "row_number": idx + 1,  # 1-based
                        "column": col,
                        "check_id": r.check_id,
                        "severity": r.severity,
                        "value": str(df_raw[col].iloc[idx]) if col in df_raw.columns else "",
                        "detail": r.message[:200],
                    })

        if not flags:
            return pd.DataFrame(columns=["row_number", "column", "check_id", "severity", "value", "detail"])

        flag_df = pd.DataFrame(flags)
        flag_df = flag_df.sort_values(
            ["severity", "row_number"],
            key=lambda x: x.map({"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4})
            if x.name == "severity" else x
        ).reset_index(drop=True)

        return flag_df

    def export(
        self,
        flag_df: pd.DataFrame,
        output_path: str,
    ):
        """Exporta las filas flaggeadas a CSV."""
        flag_df.to_csv(output_path, index=False, encoding="utf-8")

    def _get_flagged_indices(
        self,
        result: CheckResult,
        df_raw: pd.DataFrame,
        df: pd.DataFrame,
        col: str,
    ) -> List[int]:
        """Obtiene los índices de filas problemáticas para un check específico."""

        # Si el resultado ya tiene índices en metadata
        if "violation_indices" in result.metadata:
            return result.metadata["violation_indices"][:100]
        if "flagged_indices" in result.metadata:
            return result.metadata["flagged_indices"][:100]

        check_id = result.check_id
        series = df[col] if col in df.columns else None
        series_raw = df_raw[col] if col in df_raw.columns else None

        if series is None:
            return []

        # Nulls
        if check_id == "NULL_RATE":
            return series[series.isna()].index.tolist()[:100]

        # Duplicates
        if check_id == "DUPLICATE_ROWS":
            dups = df_raw.duplicated(keep="first")
            return dups[dups].index.tolist()[:100]

        # Outliers IQR
        if check_id in ("OUTLIER_IQR", "OUTLIER_ZSCORE", "OUTLIER_MODIFIED_Z"):
            s = pd.to_numeric(series, errors="coerce").dropna()
            if len(s) > 0:
                if check_id == "OUTLIER_IQR":
                    q1, q3 = s.quantile(0.25), s.quantile(0.75)
                    iqr = q3 - q1
                    if iqr > 0:
                        mask = (s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)
                        return s[mask].index.tolist()[:100]
                elif check_id == "OUTLIER_ZSCORE":
                    z = (s - s.mean()) / s.std()
                    return s[z.abs() > 3].index.tolist()[:100]
                elif check_id == "OUTLIER_MODIFIED_Z":
                    median = s.median()
                    mad = (s - median).abs().median()
                    if mad > 0:
                        mz = 0.6745 * (s - median) / mad
                        return s[mz.abs() > 3.5].index.tolist()[:100]

        # Whitespace
        if check_id == "WHITESPACE_ISSUES" and series_raw is not None:
            mask = series_raw.astype(str).apply(lambda x: x != x.strip() and x.strip() != "")
            return series_raw[mask].index.tolist()[:100]

        # Date future / ancient
        if check_id == "DATE_FUTURE":
            dt = pd.to_datetime(series, errors="coerce")
            future = dt[dt > pd.Timestamp.now()]
            return future.index.tolist()[:100]

        if check_id == "DATE_ANCIENT":
            dt = pd.to_datetime(series, errors="coerce")
            ancient = dt[dt < pd.Timestamp("1900-01-01")]
            return ancient.index.tolist()[:100]

        # Negative values
        if check_id == "NEGATIVE_VALUES":
            s = pd.to_numeric(series, errors="coerce")
            return s[s < 0].index.tolist()[:100]

        # Generic: si hay affected_count pero no podemos identificar filas exactas
        return []
