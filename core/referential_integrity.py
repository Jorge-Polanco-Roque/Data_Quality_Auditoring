"""
Referential Integrity — valida foreign keys entre CSVs en modo batch.

Formato YAML (en schema):
  foreign_keys:
    - child_table: orders.csv
      child_column: customer_id
      parent_table: customers.csv
      parent_column: id
"""

from typing import List, Dict, Any, Optional

import pandas as pd

from models.check_result import CheckResult


class ReferentialIntegrityChecker:
    """Valida integridad referencial entre DataFrames en batch mode."""

    def __init__(self, fk_rules: List[Dict[str, str]]):
        """
        Args:
            fk_rules: lista de dicts con keys:
                child_table, child_column, parent_table, parent_column
        """
        self.fk_rules = fk_rules

    def validate(self, dataframes: Dict[str, pd.DataFrame]) -> List[CheckResult]:
        """Valida FK entre los DataFrames cargados.

        Args:
            dataframes: dict {filename: DataFrame}

        Returns:
            Lista de CheckResult con las violaciones encontradas.
        """
        results = []

        for rule in self.fk_rules:
            child_table = rule.get("child_table", "")
            child_col = rule.get("child_column", "")
            parent_table = rule.get("parent_table", "")
            parent_col = rule.get("parent_column", "")

            # Buscar DataFrames
            child_df = dataframes.get(child_table)
            parent_df = dataframes.get(parent_table)

            if child_df is None:
                results.append(CheckResult(
                    check_id="FK_TABLE_NOT_FOUND", column=child_col,
                    passed=False, severity="HIGH",
                    value=0.0, threshold=0.0,
                    message=f"Tabla child '{child_table}' no encontrada para validar FK",
                    metadata={"rule": rule},
                ))
                continue

            if parent_df is None:
                results.append(CheckResult(
                    check_id="FK_TABLE_NOT_FOUND", column=parent_col,
                    passed=False, severity="HIGH",
                    value=0.0, threshold=0.0,
                    message=f"Tabla parent '{parent_table}' no encontrada para validar FK",
                    metadata={"rule": rule},
                ))
                continue

            if child_col not in child_df.columns:
                results.append(CheckResult(
                    check_id="FK_COLUMN_NOT_FOUND", column=child_col,
                    passed=False, severity="HIGH",
                    value=0.0, threshold=0.0,
                    message=f"Columna '{child_col}' no encontrada en '{child_table}'",
                    metadata={"rule": rule},
                ))
                continue

            if parent_col not in parent_df.columns:
                results.append(CheckResult(
                    check_id="FK_COLUMN_NOT_FOUND", column=parent_col,
                    passed=False, severity="HIGH",
                    value=0.0, threshold=0.0,
                    message=f"Columna '{parent_col}' no encontrada en '{parent_table}'",
                    metadata={"rule": rule},
                ))
                continue

            # Validar referential integrity
            child_values = child_df[child_col].dropna().unique()
            parent_values = set(parent_df[parent_col].dropna().unique())

            orphans = [v for v in child_values if v not in parent_values]
            orphan_count = len(orphans)
            total = len(child_values)
            orphan_pct = orphan_count / total if total > 0 else 0.0

            passed = orphan_count == 0

            if orphan_count > 0:
                severity = "CRITICAL" if orphan_pct > 0.10 else "HIGH" if orphan_pct > 0.01 else "MEDIUM"
            else:
                severity = "PASS"

            results.append(CheckResult(
                check_id="FK_VIOLATION",
                column=f"{child_table}.{child_col}",
                passed=passed,
                severity=severity,
                value=round(orphan_pct, 4),
                threshold=0.0,
                message=f"FK {child_table}.{child_col} → {parent_table}.{parent_col}: "
                        f"{orphan_count:,} valores huérfanos de {total:,} ({orphan_pct:.1%})",
                affected_count=orphan_count,
                affected_pct=orphan_pct,
                sample_values=[str(v) for v in orphans[:10]],
                metadata={
                    "child_table": child_table,
                    "child_column": child_col,
                    "parent_table": parent_table,
                    "parent_column": parent_col,
                    "orphan_count": orphan_count,
                },
            ))

        return results
