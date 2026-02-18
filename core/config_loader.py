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

from typing import Dict, Any, Optional


class ConfigLoader:
    """Carga y aplica configuración desde YAML."""

    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        """Carga un archivo YAML de configuración."""
        try:
            import yaml
        except ImportError:
            raise ImportError("Se requiere PyYAML para configuración: pip install pyyaml")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

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
