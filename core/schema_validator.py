"""
Schema validator: valida un DataFrame contra un schema YAML definido por el usuario.

Formato del schema YAML:
  columns:
    nombre_columna:
      type: numeric|categorical|date|text|boolean|email|phone
      required: true/false
      min: 0
      max: 100
      allowed_values: [a, b, c]
      pattern: "^[A-Z]{3}-\\d+$"
      not_null: true/false
      unique: true/false
  composite_keys:
    - [col1, col2]
    - [col3, col4, col5]
"""

import re
from typing import Dict, List

import pandas as pd
import numpy as np

from models.check_result import CheckResult
from models.semantic_type import SemanticType


EXPECTED_TYPE_MAP = {
    "numeric": (SemanticType.NUMERIC_CONTINUOUS, SemanticType.NUMERIC_DISCRETE),
    "categorical": (SemanticType.CATEGORICAL,),
    "date": (SemanticType.DATE, SemanticType.DATETIME),
    "text": (SemanticType.HIGH_CARDINALITY,),
    "boolean": (SemanticType.BOOLEAN,),
    "email": (SemanticType.EMAIL,),
    "phone": (SemanticType.PHONE,),
    "id": (SemanticType.ID_CANDIDATE,),
}


class SchemaValidator:
    """Valida un DataFrame contra un schema YAML."""

    def __init__(self, schema: dict):
        self.schema = schema
        self.columns_schema = schema.get("columns", {})
        self.composite_keys = schema.get("composite_keys", [])

    def validate(self, df_raw, df, column_types) -> List[CheckResult]:
        results = []

        # Validar columnas esperadas existen
        results.extend(self._check_missing_columns(df))

        # Validar columnas inesperadas
        results.extend(self._check_extra_columns(df))

        # Validar cada columna definida
        for col_name, col_schema in self.columns_schema.items():
            if col_name not in df.columns:
                continue
            results.extend(self._validate_column(df_raw, df, col_name, col_schema, column_types))

        # Validar composite keys
        for key_cols in self.composite_keys:
            results.extend(self._check_composite_key(df, key_cols))

        return results

    def _check_missing_columns(self, df) -> List[CheckResult]:
        results = []
        for col_name, col_schema in self.columns_schema.items():
            if col_schema.get("required", False) and col_name not in df.columns:
                results.append(CheckResult(
                    check_id="SCHEMA_MISSING_COLUMN", column=col_name,
                    passed=False, severity="CRITICAL",
                    value=0.0, threshold=0.0,
                    message=f"Columna requerida '{col_name}' no encontrada en el dataset",
                ))
        return results

    def _check_extra_columns(self, df) -> List[CheckResult]:
        results = []
        expected = set(self.columns_schema.keys())
        if not expected:
            return results
        extra = set(df.columns) - expected
        if extra:
            results.append(CheckResult(
                check_id="SCHEMA_EXTRA_COLUMNS", column="__dataset__",
                passed=True, severity="INFO",
                value=float(len(extra)), threshold=0.0,
                message=f"{len(extra)} columna(s) no definidas en schema: {', '.join(sorted(extra)[:10])}",
                sample_values=sorted(extra)[:10],
            ))
        return results

    def _validate_column(self, df_raw, df, col_name, col_schema, column_types) -> List[CheckResult]:
        results = []

        # Type check
        expected_type = col_schema.get("type")
        if expected_type and expected_type in EXPECTED_TYPE_MAP:
            actual_type = column_types.get(col_name)
            if actual_type and actual_type not in EXPECTED_TYPE_MAP[expected_type]:
                results.append(CheckResult(
                    check_id="SCHEMA_TYPE_MISMATCH", column=col_name,
                    passed=False, severity="HIGH",
                    value=0.0, threshold=0.0,
                    message=f"Tipo esperado '{expected_type}' pero detectado '{actual_type.value}'",
                    metadata={"expected": expected_type, "actual": actual_type.value},
                ))

        # Not null check
        if col_schema.get("not_null", False):
            null_count = int(df[col_name].isna().sum())
            if null_count > 0:
                results.append(CheckResult(
                    check_id="SCHEMA_NOT_NULL", column=col_name,
                    passed=False, severity="HIGH",
                    value=float(null_count), threshold=0.0,
                    message=f"{null_count:,} nulos encontrados en columna que debe ser not-null",
                    affected_count=null_count,
                    affected_pct=null_count / len(df) if len(df) > 0 else 0.0,
                ))

        # Unique check
        if col_schema.get("unique", False):
            dup_count = int(df[col_name].dropna().duplicated().sum())
            if dup_count > 0:
                results.append(CheckResult(
                    check_id="SCHEMA_UNIQUE_VIOLATION", column=col_name,
                    passed=False, severity="HIGH",
                    value=float(dup_count), threshold=0.0,
                    message=f"{dup_count:,} valores duplicados en columna que debe ser única",
                    affected_count=dup_count,
                    affected_pct=dup_count / len(df) if len(df) > 0 else 0.0,
                ))

        # Min/Max range
        min_val = col_schema.get("min")
        max_val = col_schema.get("max")
        if min_val is not None or max_val is not None:
            s = pd.to_numeric(df[col_name], errors="coerce").dropna()
            if len(s) > 0:
                violations = pd.Series([False] * len(s), index=s.index)
                if min_val is not None:
                    violations |= (s < float(min_val))
                if max_val is not None:
                    violations |= (s > float(max_val))
                v_count = int(violations.sum())
                if v_count > 0:
                    results.append(CheckResult(
                        check_id="SCHEMA_RANGE_VIOLATION", column=col_name,
                        passed=False, severity="HIGH",
                        value=float(v_count), threshold=0.0,
                        message=f"{v_count:,} valores fuera del rango [{min_val}, {max_val}]",
                        affected_count=v_count,
                        affected_pct=v_count / len(df) if len(df) > 0 else 0.0,
                        sample_values=s[violations].head(5).tolist(),
                    ))

        # Allowed values
        allowed = col_schema.get("allowed_values")
        if allowed:
            allowed_set = set(str(v) for v in allowed)
            actual = df[col_name].astype(str).str.strip()
            invalid = actual[~actual.isin(allowed_set) & (actual != "") & (actual != "nan")]
            v_count = len(invalid)
            if v_count > 0:
                results.append(CheckResult(
                    check_id="SCHEMA_ALLOWED_VALUES", column=col_name,
                    passed=False, severity="HIGH",
                    value=float(v_count), threshold=0.0,
                    message=f"{v_count:,} valores no permitidos",
                    affected_count=v_count,
                    affected_pct=v_count / len(df) if len(df) > 0 else 0.0,
                    sample_values=invalid.unique()[:5].tolist(),
                ))

        # Pattern (regex)
        pattern = col_schema.get("pattern")
        if pattern:
            regex = re.compile(pattern)
            raw = df_raw[col_name].astype(str).str.strip()
            non_empty = raw[(raw != "") & (raw != "nan")]
            no_match = non_empty[~non_empty.apply(lambda x: bool(regex.match(x)))]
            v_count = len(no_match)
            if v_count > 0:
                results.append(CheckResult(
                    check_id="SCHEMA_PATTERN_VIOLATION", column=col_name,
                    passed=False, severity="MEDIUM",
                    value=float(v_count), threshold=0.0,
                    message=f"{v_count:,} valores no cumplen patrón '{pattern}'",
                    affected_count=v_count,
                    affected_pct=v_count / len(df) if len(df) > 0 else 0.0,
                    sample_values=no_match.head(5).tolist(),
                ))

        return results

    def _check_composite_key(self, df, key_cols) -> List[CheckResult]:
        results = []
        # Verificar que todas las columnas existen
        missing = [c for c in key_cols if c not in df.columns]
        if missing:
            return results

        dup = df.duplicated(subset=key_cols, keep="first")
        dup_count = int(dup.sum())
        if dup_count > 0:
            key_str = " + ".join(key_cols)
            results.append(CheckResult(
                check_id="COMPOSITE_KEY_VIOLATION", column=key_str,
                passed=False, severity="HIGH",
                value=float(dup_count), threshold=0.0,
                message=f"{dup_count:,} filas con clave compuesta duplicada ({key_str})",
                affected_count=dup_count,
                affected_pct=dup_count / len(df) if len(df) > 0 else 0.0,
            ))
        return results
