"""
Config loader: carga configuración YAML para personalizar umbrales, checks habilitados, etc.

Formato del config YAML:
  thresholds:
    NULL_RATE:
      CRITICAL: 0.50
      HIGH: 0.20
      MEDIUM: 0.05
    OUTLIER_IQR:
      CRITICAL: 0.15
      HIGH: 0.08
  disabled_checks:
    - NORMALITY_TEST
    - BENFORD_LAW
  severity_overrides:
    TREND_CHANGE: INFO
    ADF_STATIONARITY: LOW
  scoring:
    CRITICAL: 25
    HIGH: 10
    MEDIUM: 5
    LOW: 2
  column_weights:
    amount: 3.0
    customer_id: 2.0
  business_rules:
    - name: "Price positive"
      assertion: "price >= 0"
      severity: HIGH
  foreign_keys:
    - child_table: orders.csv
      child_column: customer_id
      parent_table: customers.csv
      parent_column: id
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

VALID_SEVERITIES = {"CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO", "PASS"}
VALID_SCORING_KEYS = {"CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"}


class ConfigValidationError(ValueError):
    """Error de validación de configuración YAML."""
    pass


def _validate_config(config: Dict[str, Any]) -> None:
    """Valida la estructura y tipos del config YAML. Lanza ConfigValidationError si hay problemas."""
    errors = []

    # ── thresholds ──
    thresholds = config.get("thresholds")
    if thresholds is not None:
        if not isinstance(thresholds, dict):
            errors.append(f"'thresholds' debe ser un dict, recibido: {type(thresholds).__name__}")
        else:
            for check_id, levels in thresholds.items():
                if not isinstance(levels, dict):
                    errors.append(f"thresholds.{check_id} debe ser un dict, recibido: {type(levels).__name__}")
                    continue
                for sev, val in levels.items():
                    if sev not in VALID_SEVERITIES:
                        errors.append(f"thresholds.{check_id}.{sev}: severidad inválida (válidas: {VALID_SEVERITIES})")
                    if not isinstance(val, (int, float)):
                        errors.append(f"thresholds.{check_id}.{sev}: valor debe ser numérico, recibido: {type(val).__name__}")

    # ── disabled_checks ──
    disabled = config.get("disabled_checks")
    if disabled is not None:
        if not isinstance(disabled, list):
            errors.append(f"'disabled_checks' debe ser una lista, recibido: {type(disabled).__name__}")
        else:
            for item in disabled:
                if not isinstance(item, str):
                    errors.append(f"disabled_checks contiene valor no-string: {item!r}")

    # ── severity_overrides ──
    overrides = config.get("severity_overrides")
    if overrides is not None:
        if not isinstance(overrides, dict):
            errors.append(f"'severity_overrides' debe ser un dict, recibido: {type(overrides).__name__}")
        else:
            for check_id, sev in overrides.items():
                if not isinstance(sev, str) or sev not in VALID_SEVERITIES:
                    errors.append(
                        f"severity_overrides.{check_id}: '{sev}' no es severidad válida (válidas: {VALID_SEVERITIES})"
                    )

    # ── scoring ──
    scoring = config.get("scoring")
    if scoring is not None:
        if not isinstance(scoring, dict):
            errors.append(f"'scoring' debe ser un dict, recibido: {type(scoring).__name__}")
        else:
            for key, val in scoring.items():
                if key not in VALID_SCORING_KEYS:
                    errors.append(f"scoring.{key}: clave inválida (válidas: {VALID_SCORING_KEYS})")
                if not isinstance(val, (int, float)):
                    errors.append(f"scoring.{key}: valor debe ser numérico, recibido: {type(val).__name__}")
                elif val < 0:
                    errors.append(f"scoring.{key}: valor debe ser >= 0, recibido: {val}")

    # ── column_weights ──
    weights = config.get("column_weights")
    if weights is not None:
        if not isinstance(weights, dict):
            errors.append(f"'column_weights' debe ser un dict, recibido: {type(weights).__name__}")
        else:
            for col, w in weights.items():
                if not isinstance(w, (int, float)):
                    errors.append(f"column_weights.{col}: peso debe ser numérico, recibido: {type(w).__name__}")
                elif w < 0:
                    errors.append(f"column_weights.{col}: peso debe ser >= 0, recibido: {w}")

    # ── business_rules ──
    rules = config.get("business_rules")
    if rules is not None:
        if not isinstance(rules, list):
            errors.append(f"'business_rules' debe ser una lista, recibido: {type(rules).__name__}")
        else:
            for i, rule in enumerate(rules):
                if not isinstance(rule, dict):
                    errors.append(f"business_rules[{i}]: debe ser un dict")
                    continue
                if "assertion" not in rule:
                    errors.append(f"business_rules[{i}]: falta campo obligatorio 'assertion'")
                sev = rule.get("severity")
                if sev is not None and sev not in VALID_SEVERITIES:
                    errors.append(f"business_rules[{i}]: severidad '{sev}' inválida")

    # ── foreign_keys ──
    fks = config.get("foreign_keys")
    if fks is not None:
        if not isinstance(fks, list):
            errors.append(f"'foreign_keys' debe ser una lista, recibido: {type(fks).__name__}")
        else:
            required_fk_fields = {"child_table", "child_column", "parent_table", "parent_column"}
            for i, fk in enumerate(fks):
                if not isinstance(fk, dict):
                    errors.append(f"foreign_keys[{i}]: debe ser un dict")
                    continue
                missing = required_fk_fields - set(fk.keys())
                if missing:
                    errors.append(f"foreign_keys[{i}]: faltan campos: {missing}")

    if errors:
        msg = "Errores de validación en configuración YAML:\n  - " + "\n  - ".join(errors)
        raise ConfigValidationError(msg)


class ConfigLoader:
    """Carga y aplica configuración desde YAML."""

    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        """Carga un archivo YAML de configuración con validación."""
        try:
            import yaml
        except ImportError:
            raise ImportError("Se requiere PyYAML para configuración: pip install pyyaml")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        if not isinstance(config, dict):
            raise ConfigValidationError(
                f"El archivo de configuración debe contener un dict YAML, recibido: {type(config).__name__}"
            )

        _validate_config(config)
        logger.info("Configuración cargada y validada: %s", config_path)

        return {
            "thresholds": config.get("thresholds", {}),
            "disabled_checks": set(config.get("disabled_checks", [])),
            "severity_overrides": config.get("severity_overrides", {}),
            "scoring": config.get("scoring", {}),
            "column_weights": config.get("column_weights", {}),
            "business_rules": config.get("business_rules", []),
            "foreign_keys": config.get("foreign_keys", []),
        }

    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "thresholds": {},
            "disabled_checks": set(),
            "severity_overrides": {},
            "scoring": {},
            "column_weights": {},
            "business_rules": [],
            "foreign_keys": [],
        }

    @staticmethod
    def is_check_enabled(config: Optional[Dict], check_id: str) -> bool:
        if config is None:
            return True
        return check_id not in config.get("disabled_checks", set())

    @staticmethod
    def get_threshold(config: Optional[Dict], check_id: str, default: Dict) -> Dict:
        if config is None:
            return default
        overrides = config.get("thresholds", {}).get(check_id, {})
        if overrides:
            merged = dict(default)
            merged.update(overrides)
            return merged
        return default

    @staticmethod
    def get_severity_override(config: Optional[Dict], check_id: str) -> Optional[str]:
        if config is None:
            return None
        return config.get("severity_overrides", {}).get(check_id)
