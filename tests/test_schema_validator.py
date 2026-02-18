"""Tests para core/schema_validator.py"""

import pandas as pd
import numpy as np
from models.semantic_type import SemanticType
from core.schema_validator import SchemaValidator


def test_missing_required_column():
    schema = {"columns": {"missing_col": {"required": True, "type": "numeric"}}}
    df = pd.DataFrame({"other": [1, 2, 3]})
    validator = SchemaValidator(schema)
    results = validator.validate(df.astype(str), df, {"other": SemanticType.NUMERIC_CONTINUOUS})
    missing = [r for r in results if r.check_id == "SCHEMA_MISSING_COLUMN"]
    assert len(missing) == 1
    assert missing[0].severity == "CRITICAL"


def test_range_violation():
    schema = {"columns": {"age": {"min": 0, "max": 120}}}
    df = pd.DataFrame({"age": [25, 30, -5, 150, 45]})
    validator = SchemaValidator(schema)
    results = validator.validate(df.astype(str), df, {"age": SemanticType.NUMERIC_CONTINUOUS})
    violations = [r for r in results if r.check_id == "SCHEMA_RANGE_VIOLATION"]
    assert len(violations) == 1
    assert violations[0].affected_count == 2  # -5 and 150


def test_allowed_values():
    schema = {"columns": {"status": {"allowed_values": ["active", "inactive"]}}}
    df = pd.DataFrame({"status": ["active", "inactive", "deleted", "active"]})
    validator = SchemaValidator(schema)
    results = validator.validate(df.astype(str), df, {"status": SemanticType.CATEGORICAL})
    violations = [r for r in results if r.check_id == "SCHEMA_ALLOWED_VALUES"]
    assert len(violations) == 1


def test_not_null_violation():
    schema = {"columns": {"id": {"not_null": True}}}
    df = pd.DataFrame({"id": [1, 2, np.nan, 4]})
    validator = SchemaValidator(schema)
    results = validator.validate(df.astype(str), df, {"id": SemanticType.NUMERIC_CONTINUOUS})
    violations = [r for r in results if r.check_id == "SCHEMA_NOT_NULL"]
    assert len(violations) == 1


def test_unique_violation():
    schema = {"columns": {"id": {"unique": True}}}
    df = pd.DataFrame({"id": [1, 2, 2, 3]})
    validator = SchemaValidator(schema)
    results = validator.validate(df.astype(str), df, {"id": SemanticType.NUMERIC_DISCRETE})
    violations = [r for r in results if r.check_id == "SCHEMA_UNIQUE_VIOLATION"]
    assert len(violations) == 1


def test_composite_key():
    schema = {"columns": {}, "composite_keys": [["a", "b"]]}
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    validator = SchemaValidator(schema)
    results = validator.validate(df.astype(str), df, {})
    violations = [r for r in results if r.check_id == "COMPOSITE_KEY_VIOLATION"]
    assert len(violations) == 1


def test_pattern_violation():
    schema = {"columns": {"code": {"pattern": r"^[A-Z]{3}-\d{3}$"}}}
    df = pd.DataFrame({"code": ["ABC-123", "XYZ-456", "invalid", "AB-12"]})
    validator = SchemaValidator(schema)
    results = validator.validate(df.astype(str), df, {"code": SemanticType.HIGH_CARDINALITY})
    violations = [r for r in results if r.check_id == "SCHEMA_PATTERN_VIOLATION"]
    assert len(violations) == 1
    assert violations[0].affected_count == 2  # "invalid" and "AB-12"
