from typing import List, Dict, Tuple
from collections import defaultdict

from models.check_result import CheckResult


SEVERITY_DEDUCTIONS = {
    "CRITICAL": 25,
    "HIGH": 10,
    "MEDIUM": 5,
    "LOW": 2,
    "INFO": 0,
    "PASS": 0,
}

GRADE_SCALE = [
    (90, "A"),
    (75, "B"),
    (60, "C"),
    (40, "D"),
    (0, "F"),
]


def _grade_from_score(score: float) -> str:
    for threshold, grade in GRADE_SCALE:
        if score >= threshold:
            return grade
    return "F"


class ScoringSystem:
    """Capa 5: Agrega resultados y calcula scores de salud por columna y global."""

    def __init__(self, config=None):
        self.config = config
        # Permitir override de deductions desde config
        self._deductions = dict(SEVERITY_DEDUCTIONS)
        if config and config.get("scoring"):
            for sev, val in config["scoring"].items():
                if sev in self._deductions:
                    self._deductions[sev] = val
        # Pesos por columna desde config
        self._column_weights = {}
        if config and config.get("column_weights"):
            self._column_weights = config["column_weights"]

    def calculate(
        self, results: List[CheckResult], null_pcts: Dict[str, float]
    ) -> Dict:
        """Calcula scores por columna y score global del dataset.

        Args:
            results: lista de CheckResult de todos los checks.
            null_pcts: dict {columna: pct_nulos} para ponderación.

        Returns:
            dict con column_scores, dataset_score, dataset_grade, issues_by_severity.
        """
        # Agrupar resultados por columna
        col_results = defaultdict(list)
        for r in results:
            col_results[r.column].append(r)

        # Score por columna
        column_scores = {}
        for col, col_res in col_results.items():
            score = 100.0
            for r in col_res:
                if not r.passed:
                    score -= self._deductions.get(r.severity, 0)
            score = max(0.0, score)
            grade = _grade_from_score(score)
            column_scores[col] = {
                "score": round(score, 1),
                "grade": grade,
                "checks_run": len(col_res),
                "checks_failed": sum(1 for r in col_res if not r.passed),
            }

        # Score global: media ponderada
        total_weight = 0.0
        weighted_sum = 0.0
        for col, info in column_scores.items():
            if col == "__dataset__":
                continue
            null_pct = null_pcts.get(col, 0.0)
            # Peso configurable por columna; default = 1/(1+null_pct)
            weight = self._column_weights.get(col, 1.0 / (1.0 + null_pct))
            weighted_sum += info["score"] * weight
            total_weight += weight

        dataset_score = weighted_sum / total_weight if total_weight > 0 else 0.0
        dataset_grade = _grade_from_score(dataset_score)

        # Conteo por severidad
        issues_by_severity = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
        for r in results:
            if not r.passed and r.severity in issues_by_severity:
                issues_by_severity[r.severity] += 1

        return {
            "column_scores": column_scores,
            "dataset_score": round(dataset_score, 1),
            "dataset_grade": dataset_grade,
            "issues_by_severity": issues_by_severity,
            "total_issues": sum(issues_by_severity.values()),
        }
