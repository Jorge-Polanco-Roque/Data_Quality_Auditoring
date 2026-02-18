"""
Drift Detector: compara dos versiones de un dataset para detectar cambios significativos.

Detecta: cambios de schema, shifts de distribución, cambios de cardinalidad,
cambios de null rate, nuevas columnas, columnas eliminadas.
"""

import json
from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
from scipy import stats

from core.data_loader import DataLoader


class DriftDetector:
    """Compara un dataset de referencia contra uno actual."""

    def compare(self, reference_path: str, current_path: str) -> Dict:
        """Ejecuta comparación completa entre referencia y actual."""
        loader = DataLoader()
        ref_raw, ref_df, ref_meta = loader.load(reference_path)
        cur_raw, cur_df, cur_meta = loader.load(current_path)

        report = {
            "generated_at": datetime.now().isoformat(),
            "reference": {"file": ref_meta["file_name"], "rows": ref_meta["n_rows"], "cols": ref_meta["n_cols"]},
            "current": {"file": cur_meta["file_name"], "rows": cur_meta["n_rows"], "cols": cur_meta["n_cols"]},
            "schema_changes": self._schema_diff(ref_df, cur_df),
            "column_drifts": self._column_drifts(ref_df, cur_df),
            "row_count_change": {
                "reference": ref_meta["n_rows"],
                "current": cur_meta["n_rows"],
                "change_pct": round((cur_meta["n_rows"] - ref_meta["n_rows"]) / max(ref_meta["n_rows"], 1) * 100, 2),
            },
            "summary": {},
        }

        # Build summary
        drifts = report["column_drifts"]
        significant_drifts = [d for d in drifts if d.get("has_drift")]
        report["summary"] = {
            "total_columns_compared": len(drifts),
            "columns_with_drift": len(significant_drifts),
            "new_columns": len(report["schema_changes"]["added"]),
            "removed_columns": len(report["schema_changes"]["removed"]),
            "type_changes": len(report["schema_changes"]["type_changed"]),
            "drift_severity": "HIGH" if len(significant_drifts) > len(drifts) * 0.3
                              else ("MEDIUM" if significant_drifts else "LOW"),
        }

        return report

    def _schema_diff(self, ref_df, cur_df):
        ref_cols = set(ref_df.columns)
        cur_cols = set(cur_df.columns)

        added = sorted(cur_cols - ref_cols)
        removed = sorted(ref_cols - cur_cols)
        common = sorted(ref_cols & cur_cols)

        type_changed = []
        for col in common:
            ref_dtype = str(ref_df[col].dtype)
            cur_dtype = str(cur_df[col].dtype)
            if ref_dtype != cur_dtype:
                type_changed.append({
                    "column": col,
                    "reference_dtype": ref_dtype,
                    "current_dtype": cur_dtype,
                })

        return {
            "added": added,
            "removed": removed,
            "common": common,
            "type_changed": type_changed,
        }

    def _column_drifts(self, ref_df, cur_df):
        drifts = []
        common = sorted(set(ref_df.columns) & set(cur_df.columns))

        for col in common:
            drift = {"column": col, "has_drift": False, "tests": []}

            ref_s = ref_df[col]
            cur_s = cur_df[col]

            # Null rate change
            ref_null = float(ref_s.isna().mean())
            cur_null = float(cur_s.isna().mean())
            null_change = abs(cur_null - ref_null)
            if null_change > 0.05:
                drift["tests"].append({
                    "test": "null_rate_change",
                    "reference": round(ref_null, 4),
                    "current": round(cur_null, 4),
                    "change": round(null_change, 4),
                    "significant": True,
                })
                drift["has_drift"] = True

            # Numeric drift: KS test
            if pd.api.types.is_numeric_dtype(ref_s) and pd.api.types.is_numeric_dtype(cur_s):
                ref_num = pd.to_numeric(ref_s, errors="coerce").dropna()
                cur_num = pd.to_numeric(cur_s, errors="coerce").dropna()
                if len(ref_num) >= 10 and len(cur_num) >= 10:
                    ks_stat, ks_p = stats.ks_2samp(ref_num, cur_num)
                    significant = ks_p < 0.01
                    drift["tests"].append({
                        "test": "ks_2sample",
                        "statistic": round(float(ks_stat), 6),
                        "p_value": round(float(ks_p), 6),
                        "significant": significant,
                    })
                    if significant:
                        drift["has_drift"] = True

                    # Mean/std change
                    ref_mean, cur_mean = float(ref_num.mean()), float(cur_num.mean())
                    ref_std, cur_std = float(ref_num.std()), float(cur_num.std())
                    drift["tests"].append({
                        "test": "descriptive_change",
                        "reference_mean": round(ref_mean, 4),
                        "current_mean": round(cur_mean, 4),
                        "reference_std": round(ref_std, 4),
                        "current_std": round(cur_std, 4),
                        "mean_change_pct": round(abs(cur_mean - ref_mean) / max(abs(ref_mean), 1e-10) * 100, 2),
                    })

            # Categorical drift: chi-squared
            elif ref_s.dtype == "object" or cur_s.dtype == "object":
                ref_cats = ref_s.dropna().astype(str).value_counts()
                cur_cats = cur_s.dropna().astype(str).value_counts()

                # Unificar categorías
                all_cats = sorted(set(ref_cats.index) | set(cur_cats.index))
                if len(all_cats) >= 2 and len(all_cats) <= 100:
                    ref_freq = np.array([ref_cats.get(c, 0) for c in all_cats], dtype=float)
                    cur_freq = np.array([cur_cats.get(c, 0) for c in all_cats], dtype=float)

                    # Normalizar
                    ref_freq = ref_freq / ref_freq.sum() if ref_freq.sum() > 0 else ref_freq
                    cur_freq = cur_freq / cur_freq.sum() if cur_freq.sum() > 0 else cur_freq

                    # Chi-squared test con frecuencias esperadas
                    try:
                        n_cur = len(cur_s.dropna())
                        expected = ref_freq * n_cur
                        observed = cur_freq * n_cur
                        # Evitar expected == 0
                        mask = expected > 0
                        if mask.sum() >= 2:
                            chi2, p = stats.chisquare(observed[mask], expected[mask])
                            significant = p < 0.01
                            drift["tests"].append({
                                "test": "chi2_distribution",
                                "statistic": round(float(chi2), 4),
                                "p_value": round(float(p), 6),
                                "significant": significant,
                            })
                            if significant:
                                drift["has_drift"] = True
                    except Exception:
                        pass

                # Nuevas/eliminadas categorías
                new_cats = set(cur_cats.index) - set(ref_cats.index)
                removed_cats = set(ref_cats.index) - set(cur_cats.index)
                if new_cats or removed_cats:
                    drift["tests"].append({
                        "test": "category_changes",
                        "new_categories": sorted(new_cats)[:10],
                        "removed_categories": sorted(removed_cats)[:10],
                    })
                    if len(new_cats) + len(removed_cats) > 3:
                        drift["has_drift"] = True

            # Cardinality change
            ref_nunique = ref_s.nunique()
            cur_nunique = cur_s.nunique()
            if ref_nunique > 0:
                cardinality_change = abs(cur_nunique - ref_nunique) / ref_nunique
                if cardinality_change > 0.2:
                    drift["tests"].append({
                        "test": "cardinality_change",
                        "reference": int(ref_nunique),
                        "current": int(cur_nunique),
                        "change_pct": round(cardinality_change * 100, 2),
                    })
                    drift["has_drift"] = True

            drifts.append(drift)

        return drifts

    def print_summary(self, report):
        """Imprime resumen de drift a stdout."""
        summary = report["summary"]
        print()
        print(f"{'='*50}")
        print(f"  DRIFT DETECTION REPORT")
        print(f"{'='*50}")
        print(f"  Reference: {report['reference']['file']} ({report['reference']['rows']:,} rows)")
        print(f"  Current:   {report['current']['file']} ({report['current']['rows']:,} rows)")
        print(f"  Row change: {report['row_count_change']['change_pct']:+.1f}%")
        print()
        print(f"  Schema changes:")
        print(f"    New columns: {summary['new_columns']}")
        print(f"    Removed columns: {summary['removed_columns']}")
        print(f"    Type changes: {summary['type_changes']}")
        print()
        print(f"  Distribution drift:")
        print(f"    Columns compared: {summary['total_columns_compared']}")
        print(f"    Columns with drift: {summary['columns_with_drift']}")
        print(f"    Severity: {summary['drift_severity']}")

        # Detalles de drift
        for d in report["column_drifts"]:
            if d["has_drift"]:
                tests = ", ".join(t["test"] for t in d["tests"] if t.get("significant"))
                print(f"    → {d['column']}: {tests}")
        print()
