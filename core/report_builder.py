import json
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

from models.semantic_type import SemanticType
from models.check_result import CheckResult


class ReportBuilder:
    """Capa 6: Genera reporte estandarizado JSON + texto."""

    def build(
        self,
        results: List[CheckResult],
        scoring: Dict,
        column_types: Dict[str, SemanticType],
        metadata: Dict,
        df: pd.DataFrame,
    ) -> Dict:
        """Construye el reporte completo como dict (serializable a JSON)."""
        report = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "file_analyzed": metadata.get("file_name", ""),
                "total_rows": metadata.get("n_rows", 0),
                "total_columns": metadata.get("n_cols", 0),
                "encoding": metadata.get("encoding", ""),
                "delimiter": metadata.get("delimiter", ""),
            },
            "dataset_summary": {
                "health_score": scoring["dataset_score"],
                "health_grade": scoring["dataset_grade"],
                "total_issues": scoring["total_issues"],
                "issues_by_severity": scoring["issues_by_severity"],
                "critical_columns": self._get_critical_columns(scoring),
                "clean_columns": self._get_clean_columns(scoring),
            },
            "column_profiles": self._build_column_profiles(
                results, scoring, column_types, df
            ),
            "critical_issues": self._get_critical_issues(results),
            "recommendations": self._build_recommendations(results),
            "statistical_summary": self._build_statistical_summary(df, column_types),
            "column_profiling": self._build_column_profiling(df, column_types),
        }
        return report

    def to_json(self, report: Dict, output_path: str):
        """Escribe el reporte como JSON."""
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    def to_text(self, report: Dict, output_path: Optional[str] = None) -> str:
        """Genera el reporte formateado como texto. Si output_path, lo escribe a archivo."""
        lines = []
        meta = report["report_metadata"]
        summary = report["dataset_summary"]

        lines.append("+" + "=" * 60 + "+")
        lines.append("|" + "DATA QUALITY AUDIT REPORT".center(60) + "|")
        lines.append("+" + "=" * 60 + "+")
        lines.append("")
        lines.append(f"Archivo     : {meta['file_analyzed']}")
        lines.append(f"Filas       : {meta['total_rows']:,}")
        lines.append(f"Columnas    : {meta['total_columns']}")
        lines.append(f"Generado    : {meta['generated_at']}")
        lines.append(f"Health Score: {summary['health_score']}/100  ({summary['health_grade']})")
        lines.append("")

        # Resumen de issues
        lines.append("-" * 60)
        lines.append("RESUMEN DE ISSUES")
        lines.append("-" * 60)
        sev = summary["issues_by_severity"]
        lines.append(f"  CRITICAL : {sev.get('CRITICAL', 0)}")
        lines.append(f"  HIGH     : {sev.get('HIGH', 0)}")
        lines.append(f"  MEDIUM   : {sev.get('MEDIUM', 0)}")
        lines.append(f"  LOW      : {sev.get('LOW', 0)}")
        lines.append("")

        # Puntos críticos
        critical_issues = report.get("critical_issues", [])
        if critical_issues:
            lines.append("-" * 60)
            lines.append("PUNTOS CRITICOS (requieren accion inmediata)")
            lines.append("-" * 60)
            for issue in critical_issues:
                lines.append(f"  [{issue['severity']}] {issue['column']} -> {issue['check_id']}")
                lines.append(f"  Detalle   : {issue['message']}")
                lines.append(f"  Afectados : {issue['affected_count']:,} registros ({issue['affected_pct']:.1%})")
                if issue.get("sample_values"):
                    lines.append(f"  Muestra   : {issue['sample_values'][:5]}")
                lines.append("  " + "-" * 45)
            lines.append("")

        # Reporte por columna
        lines.append("-" * 60)
        lines.append("REPORTE POR COLUMNA")
        lines.append("-" * 60)
        for col_name, profile in report.get("column_profiles", {}).items():
            score_info = f"Score: {profile['health_score']}/100 ({profile['health_grade']})"
            lines.append(f"  {col_name} [{profile['semantic_type']}] -- {score_info}")
            lines.append(f"  Nulls: {profile['null_pct']:.1%} | Unicos: {profile['n_unique']:,}")
            issues = profile.get("issues", [])
            if issues:
                issue_strs = [f"[{i['severity']}] {i['check_id']}" for i in issues]
                lines.append(f"  Issues: {', '.join(issue_strs)}")
            else:
                lines.append("  Issues: ninguno")
            lines.append("  " + "-" * 45)
        lines.append("")

        # Recomendaciones
        recs = report.get("recommendations", [])
        if recs:
            lines.append("-" * 60)
            lines.append("RECOMENDACIONES PRIORIZADAS")
            lines.append("-" * 60)
            for i, rec in enumerate(recs, 1):
                lines.append(f"  #{i}. [{rec['category']}] {rec['column']}: {rec['action']}")
            lines.append("")

        text = "\n".join(lines)

        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)

        return text

    def _get_critical_columns(self, scoring: Dict) -> List[str]:
        return [
            col for col, info in scoring["column_scores"].items()
            if info["grade"] in ("D", "F") and col != "__dataset__"
        ]

    def _get_clean_columns(self, scoring: Dict) -> List[str]:
        return [
            col for col, info in scoring["column_scores"].items()
            if info["grade"] == "A" and info["checks_failed"] == 0 and col != "__dataset__"
        ]

    def _build_column_profiles(
        self, results, scoring, column_types, df
    ) -> Dict:
        profiles = {}
        results_by_col = {}
        for r in results:
            results_by_col.setdefault(r.column, []).append(r)

        for col, sem_type in column_types.items():
            col_score = scoring["column_scores"].get(col, {"score": 100, "grade": "A", "checks_run": 0, "checks_failed": 0})
            col_results = results_by_col.get(col, [])
            failed_issues = [r.to_dict() for r in col_results if not r.passed]

            null_pct = 0.0
            if col in df.columns:
                null_pct = float(df[col].isna().mean())

            n_unique = 0
            if col in df.columns:
                n_unique = int(df[col].nunique())

            profiles[col] = {
                "semantic_type": sem_type.value,
                "pandas_dtype": str(df[col].dtype) if col in df.columns else "unknown",
                "n_unique": n_unique,
                "null_pct": round(null_pct, 4),
                "health_score": col_score["score"],
                "health_grade": col_score["grade"],
                "checks_run": col_score["checks_run"],
                "checks_failed": col_score["checks_failed"],
                "issues": failed_issues,
            }
        return profiles

    def _get_critical_issues(self, results: List[CheckResult]) -> List[Dict]:
        critical = [r for r in results if r.severity in ("CRITICAL", "HIGH") and not r.passed]
        critical.sort(key=lambda r: {"CRITICAL": 0, "HIGH": 1}.get(r.severity, 2))
        return [r.to_dict() for r in critical]

    def _build_recommendations(self, results: List[CheckResult]) -> List[Dict]:
        recs = []
        severity_priority = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}

        failed = [r for r in results if not r.passed and r.severity in severity_priority]
        failed.sort(key=lambda r: severity_priority.get(r.severity, 5))

        seen = set()
        for r in failed:
            key = (r.column, r.check_id)
            if key in seen:
                continue
            seen.add(key)

            action = self._recommend_action(r)
            recs.append({
                "priority": len(recs) + 1,
                "category": self._categorize_check(r.check_id),
                "column": r.column,
                "action": action,
                "estimated_impact": r.severity,
            })

            if len(recs) >= 20:
                break

        return recs

    def _recommend_action(self, result: CheckResult) -> str:
        actions = {
            "NULL_RATE": "Investigar fuente de nulos; evaluar imputacion o exclusion",
            "DUPLICATE_ROWS": "Eliminar filas duplicadas o investigar origen",
            "OUTLIER_IQR": "Revisar outliers: errores de captura o valores legitimos extremos",
            "OUTLIER_ZSCORE": "Revisar outliers por Z-score: posibles errores de medicion",
            "OUTLIER_MODIFIED_Z": "Revisar outliers robustos: distribución sesgada con valores extremos",
            "DISTRIBUTION_SKEW": "Evaluar si la asimetria afecta el analisis; considerar transformacion",
            "DISTRIBUTION_KURTOSIS": "Distribución con colas pesadas; verificar valores extremos",
            "TREND_CHANGE": "Investigar cambio de tendencia: posible evento externo o error de pipeline",
            "DATE_FORMAT_MIX": "Estandarizar formato de fecha a ISO 8601",
            "DATE_FUTURE": "Verificar fechas futuras: posible error de captura",
            "DATE_ANCIENT": "Verificar fechas anteriores a 1900: posible error de dato",
            "ID_DUPLICATES": "IDs duplicados: verificar integridad referencial",
            "ID_NULL": "IDs nulos: critico para integridad de datos",
            "CASE_INCONSISTENCY": "Normalizar capitalización de categorías",
            "TYPO_CANDIDATES": "Revisar categorías similares: posibles errores tipográficos",
            "EMAIL_FORMAT": "Validar y corregir formato de emails",
            "PHONE_FORMAT": "Estandarizar formato de teléfonos",
            "TEMPORAL_DRIFT": "Investigar cambio en distribución temporal",
            "CLASS_IMBALANCE": "Verificar si el desbalance de clases es esperado",
            "WHITESPACE_ISSUES": "Aplicar trim a valores con espacios leading/trailing",
        }
        return actions.get(result.check_id, f"Revisar issue {result.check_id} en columna {result.column}")

    def _categorize_check(self, check_id: str) -> str:
        categories = {
            "NULL_RATE": "Missing Data",
            "DUPLICATE_ROWS": "Duplicados",
            "WHITESPACE_ISSUES": "Formato",
            "CONSTANT_COLUMN": "Relevancia",
            "NEAR_CONSTANT": "Relevancia",
            "OUTLIER_IQR": "Outliers",
            "OUTLIER_ZSCORE": "Outliers",
            "OUTLIER_MODIFIED_Z": "Outliers",
            "DISTRIBUTION_SKEW": "Distribución",
            "DISTRIBUTION_KURTOSIS": "Distribución",
            "NEGATIVE_VALUES": "Rango",
            "ZERO_VALUES": "Rango",
            "TREND_CHANGE": "Tendencia",
            "VARIANCE_SUDDEN_CHANGE": "Tendencia",
            "DATE_FORMAT_MIX": "Formato Fecha",
            "DATE_FUTURE": "Validez Fecha",
            "DATE_ANCIENT": "Validez Fecha",
            "DATE_SEQUENCE_GAPS": "Continuidad Temporal",
            "TEMPORAL_DRIFT": "Drift",
            "RARE_CATEGORIES": "Categorías",
            "CASE_INCONSISTENCY": "Formato Categorías",
            "TYPO_CANDIDATES": "Calidad Categorías",
            "CLASS_IMBALANCE": "Balance",
            "EMAIL_FORMAT": "Formato Contacto",
            "PHONE_FORMAT": "Formato Contacto",
            "ID_DUPLICATES": "Integridad ID",
            "ID_NULL": "Integridad ID",
            "ID_FORMAT_CONSISTENCY": "Formato ID",
        }
        return categories.get(check_id, "General")

    def _build_statistical_summary(self, df: pd.DataFrame, column_types: Dict[str, SemanticType]) -> Dict:
        summary: Dict = {"numeric_columns": {}, "categorical_columns": {}, "date_columns": {}}

        for col, sem_type in column_types.items():
            if col not in df.columns:
                continue

            if sem_type in (SemanticType.NUMERIC_CONTINUOUS, SemanticType.NUMERIC_DISCRETE):
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if len(s) > 0:
                    q1, q3 = s.quantile(0.25), s.quantile(0.75)
                    iqr = q3 - q1
                    outlier_iqr = int(((s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)).sum()) if iqr > 0 else 0
                    mean, std = s.mean(), s.std()
                    outlier_z = int((((s - mean) / std).abs() > 3).sum()) if std > 0 else 0

                    summary["numeric_columns"][col] = {
                        "mean": round(float(s.mean()), 4),
                        "median": round(float(s.median()), 4),
                        "std": round(float(s.std()), 4),
                        "min": round(float(s.min()), 4),
                        "max": round(float(s.max()), 4),
                        "skewness": round(float(s.skew()), 4),
                        "kurtosis": round(float(s.kurtosis()), 4),
                        "outlier_count_iqr": outlier_iqr,
                        "outlier_count_zscore": outlier_z,
                    }

            elif sem_type in (SemanticType.CATEGORICAL, SemanticType.BOOLEAN):
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    vc = non_null.value_counts()
                    rare = vc[vc / len(non_null) < 0.005]
                    summary["categorical_columns"][col] = {
                        "n_unique": int(non_null.nunique()),
                        "top_value": str(vc.index[0]),
                        "top_freq": round(float(vc.iloc[0] / len(non_null)), 4),
                        "rare_categories": [str(c) for c in rare.index[:10]],
                    }

            elif sem_type in (SemanticType.DATE, SemanticType.DATETIME):
                dt = pd.to_datetime(df[col], errors="coerce").dropna()
                if len(dt) > 0:
                    diffs = dt.sort_values().diff().dropna()
                    gap_count = 0
                    if len(diffs) > 0:
                        median_diff = diffs.median()
                        if median_diff > pd.Timedelta(0):
                            gap_count = int((diffs > 3 * median_diff).sum())

                    summary["date_columns"][col] = {
                        "min_date": str(dt.min()),
                        "max_date": str(dt.max()),
                        "formats_found": [],  # Populated by DATE_FORMAT_MIX check
                        "gap_count": gap_count,
                    }

        return summary

    def _build_column_profiling(self, df: pd.DataFrame, column_types: Dict[str, SemanticType]) -> Dict:
        """Profiling detallado por columna: percentiles, histograma textual, top valores, CV."""
        profiling = {}

        for col, sem_type in column_types.items():
            if col not in df.columns:
                continue

            profile = {}

            if sem_type in (SemanticType.NUMERIC_CONTINUOUS, SemanticType.NUMERIC_DISCRETE):
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if len(s) > 0:
                    pcts = s.quantile([0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
                    profile["percentiles"] = {
                        f"p{int(k*100)}": round(float(v), 4) for k, v in pcts.items()
                    }
                    q1, q3 = s.quantile(0.25), s.quantile(0.75)
                    profile["iqr"] = round(float(q3 - q1), 4)
                    mean = s.mean()
                    std = s.std()
                    profile["cv"] = round(float(std / mean), 4) if mean != 0 else None

                    # Histograma textual (10 bins)
                    try:
                        counts, edges = np.histogram(s, bins=10)
                        max_count = max(counts) if len(counts) > 0 else 1
                        hist = []
                        for i, c in enumerate(counts):
                            bar_len = int(20 * c / max_count) if max_count > 0 else 0
                            hist.append({
                                "range": f"{edges[i]:.2f}-{edges[i+1]:.2f}",
                                "count": int(c),
                                "bar": "█" * bar_len,
                            })
                        profile["histogram"] = hist
                    except Exception:
                        pass

            # Top valores (para cualquier tipo)
            non_null = df[col].dropna()
            if len(non_null) > 0:
                vc = non_null.value_counts().head(10)
                profile["top_values"] = [
                    (str(val), int(cnt)) for val, cnt in vc.items()
                ]

            if profile:
                profiling[col] = profile

        return profiling
