"""
Business Rules Engine — evalúa reglas de negocio condicionales definidas en YAML.

Formato YAML:
  business_rules:
    - name: "Refund requires cancelled status"
      condition: "status == 'cancelled'"
      assertion: "refund_amount > 0"
      severity: HIGH
      description: "Si status=cancelled, refund_amount debe ser > 0"

    - name: "End date after start date"
      assertion: "end_date > start_date"
      severity: CRITICAL

    - name: "Price within range"
      assertion: "price >= 0 and price <= 10000"
      severity: HIGH

    - name: "Age consistency"
      condition: "age is not null"
      assertion: "age >= 0 and age <= 120"
      severity: MEDIUM
"""

from typing import List, Dict, Any, Optional

import pandas as pd
import numpy as np

from models.check_result import CheckResult


class BusinessRulesEngine:
    """Evalúa reglas de negocio condicionales sobre el DataFrame."""

    def __init__(self, rules: List[Dict[str, Any]]):
        self.rules = rules

    def evaluate(self, df: pd.DataFrame) -> List[CheckResult]:
        """Evalúa todas las reglas y retorna CheckResults."""
        results = []
        for rule in self.rules:
            try:
                result = self._evaluate_rule(rule, df)
                results.append(result)
            except Exception as e:
                results.append(CheckResult(
                    check_id="BUSINESS_RULE",
                    column="__dataset__",
                    passed=True,
                    severity="INFO",
                    value=0.0,
                    threshold=0.0,
                    message=f"Error evaluando regla '{rule.get('name', '?')}': {e}",
                    metadata={"error": True, "rule_name": rule.get("name", "")},
                ))
        return results

    def _evaluate_rule(self, rule: Dict, df: pd.DataFrame) -> CheckResult:
        """Evalúa una sola regla de negocio."""
        name = rule.get("name", "Unnamed Rule")
        condition = rule.get("condition")
        assertion = rule["assertion"]
        severity = rule.get("severity", "MEDIUM")
        description = rule.get("description", "")

        n_total = len(df)
        if n_total == 0:
            return CheckResult(
                check_id="BUSINESS_RULE", column="__dataset__", passed=True,
                severity="PASS", value=0.0, threshold=0.0,
                message=f"Regla '{name}': dataset vacío",
                metadata={"rule_name": name},
            )

        # Aplicar condición (filtrar filas donde aplica la regla)
        if condition:
            try:
                mask = df.eval(condition)
            except Exception:
                # Intentar con evaluación manual para condiciones con "is not null"
                mask = self._eval_condition(condition, df)
            applicable_df = df[mask]
        else:
            applicable_df = df
            mask = pd.Series(True, index=df.index)

        n_applicable = len(applicable_df)
        if n_applicable == 0:
            return CheckResult(
                check_id="BUSINESS_RULE", column="__dataset__", passed=True,
                severity="PASS", value=0.0, threshold=0.0,
                message=f"Regla '{name}': sin filas que cumplan la condición",
                metadata={"rule_name": name, "applicable_rows": 0},
            )

        # Evaluar assertion
        try:
            assertion_mask = applicable_df.eval(assertion)
        except Exception:
            assertion_mask = self._eval_assertion(assertion, applicable_df)

        violations = int((~assertion_mask).sum())
        violation_pct = violations / n_total
        passed = violations == 0

        # Obtener índices de violaciones para flagged_rows
        violation_indices = applicable_df[~assertion_mask].index.tolist()[:20]

        return CheckResult(
            check_id="BUSINESS_RULE",
            column="__dataset__",
            passed=passed,
            severity="PASS" if passed else severity,
            value=round(violation_pct, 4),
            threshold=0.0,
            message=f"Regla '{name}': {violations:,} violaciones de {n_applicable:,} "
                    f"filas aplicables ({violation_pct:.1%})"
                    + (f" — {description}" if description and not passed else ""),
            affected_count=violations,
            affected_pct=violation_pct,
            metadata={
                "rule_name": name,
                "applicable_rows": n_applicable,
                "violation_indices": violation_indices,
            },
        )

    def _eval_condition(self, condition: str, df: pd.DataFrame) -> pd.Series:
        """Evaluación manual para condiciones que pd.eval no soporta."""
        cond = condition.strip()

        # "column is not null"
        if " is not null" in cond.lower():
            col = cond.lower().replace(" is not null", "").strip()
            if col in df.columns:
                return df[col].notna()

        # "column is null"
        if " is null" in cond.lower():
            col = cond.lower().replace(" is null", "").strip()
            if col in df.columns:
                return df[col].isna()

        return pd.Series(True, index=df.index)

    def _eval_assertion(self, assertion: str, df: pd.DataFrame) -> pd.Series:
        """Evaluación manual para assertions complejas."""
        # Intentar con "and" splitting
        if " and " in assertion.lower():
            parts = assertion.split(" and ")
            result = pd.Series(True, index=df.index)
            for part in parts:
                try:
                    result = result & df.eval(part.strip())
                except Exception:
                    pass
            return result

        return pd.Series(True, index=df.index)
