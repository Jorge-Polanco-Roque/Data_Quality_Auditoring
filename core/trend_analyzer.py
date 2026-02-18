"""
Trend Analyzer — compara scores históricos del mismo CSV para detectar tendencias de calidad.
Lee corridas anteriores desde outputs/ para el mismo archivo.
"""

import os
import json
import glob
from typing import List, Dict, Optional

OUTPUTS_DIR = "outputs"


class TrendAnalyzer:
    """Analiza tendencia de calidad comparando corridas históricas."""

    def get_history(self, csv_basename: str) -> List[Dict]:
        """Busca reportes anteriores del mismo CSV en outputs/.

        Returns:
            Lista de dicts con: run_dir, generated_at, health_score, health_grade,
            total_issues, issues_by_severity
        """
        history = []

        if not os.path.exists(OUTPUTS_DIR):
            return history

        # Buscar carpetas que correspondan a este CSV
        # Pattern: NNN_basename (sin la extensión)
        pattern = os.path.join(OUTPUTS_DIR, f"[0-9][0-9][0-9]_{csv_basename}")
        matching_dirs = sorted(glob.glob(pattern))

        for run_dir in matching_dirs:
            report_path = os.path.join(run_dir, "report.json")
            if not os.path.exists(report_path):
                continue

            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    report = json.load(f)

                summary = report.get("dataset_summary", {})
                meta = report.get("report_metadata", {})

                history.append({
                    "run_dir": os.path.basename(run_dir),
                    "generated_at": meta.get("generated_at", ""),
                    "health_score": summary.get("health_score", 0),
                    "health_grade": summary.get("health_grade", "?"),
                    "total_issues": summary.get("total_issues", 0),
                    "issues_by_severity": summary.get("issues_by_severity", {}),
                    "total_rows": meta.get("total_rows", 0),
                    "total_columns": meta.get("total_columns", 0),
                })
            except (json.JSONDecodeError, KeyError, OSError):
                continue

        return history

    def build_trend_report(self, csv_basename: str, current_score: float, current_grade: str) -> Optional[Dict]:
        """Genera reporte de tendencia comparando con corridas anteriores.

        Returns:
            None si no hay historial, o dict con trend info.
        """
        history = self.get_history(csv_basename)

        if not history:
            return None

        scores = [h["health_score"] for h in history]

        # Agregar el score actual al final
        all_scores = scores + [current_score]

        trend_data = {
            "previous_runs": len(history),
            "history": history,
            "current_score": current_score,
            "current_grade": current_grade,
            "score_history": all_scores,
        }

        # Calcular tendencia
        if len(scores) >= 1:
            last_score = scores[-1]
            delta = current_score - last_score

            if delta > 5:
                trend = "IMPROVING"
                trend_desc = f"Mejora de {delta:+.1f} puntos vs corrida anterior"
            elif delta < -5:
                trend = "DEGRADING"
                trend_desc = f"Degradación de {delta:+.1f} puntos vs corrida anterior"
            else:
                trend = "STABLE"
                trend_desc = f"Estable ({delta:+.1f} puntos vs corrida anterior)"

            trend_data["trend"] = trend
            trend_data["trend_description"] = trend_desc
            trend_data["delta_vs_previous"] = round(delta, 1)

        if len(scores) >= 2:
            avg_previous = sum(scores) / len(scores)
            trend_data["avg_previous_score"] = round(avg_previous, 1)
            trend_data["best_score"] = max(scores)
            trend_data["worst_score"] = min(scores)

        return trend_data
