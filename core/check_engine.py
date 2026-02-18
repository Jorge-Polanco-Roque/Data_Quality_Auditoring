import logging
import sys
from typing import List, Dict, Optional

import pandas as pd

from models.semantic_type import SemanticType
from models.check_result import CheckResult
from core.check_registry import CheckRegistry
from core.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class CheckEngine:
    """Capa 4: Ejecuta todos los checks aplicables de forma segura."""

    def __init__(self, config=None):
        self.registry = CheckRegistry()
        self._duplicate_checked = False
        self.config = config

    def run_all(
        self,
        df_raw: pd.DataFrame,
        df: pd.DataFrame,
        column_types: Dict[str, SemanticType],
        date_col: Optional[str] = None,
    ) -> List[CheckResult]:
        """Ejecuta todos los checks para todas las columnas según su tipo semántico."""
        results = []

        # Metadata compartida
        base_metadata = {
            "_df_raw": df_raw,
            "_df": df,
            "_date_col": date_col,
        }

        for col, sem_type in column_types.items():
            checks = self.registry.get_checks_for_type(sem_type)
            series_raw = df_raw[col] if col in df_raw.columns else pd.Series(name=col, dtype=str)
            series_typed = df[col] if col in df.columns else series_raw

            for check_def in checks:
                check_id = check_def["check_id"]
                func = check_def["function"]

                # Check si está deshabilitado por config
                if not ConfigLoader.is_check_enabled(self.config, check_id):
                    continue

                # DUPLICATE_ROWS solo se ejecuta una vez (nivel dataset)
                if check_id == "DUPLICATE_ROWS":
                    if self._duplicate_checked:
                        continue
                    self._duplicate_checked = True

                result = self._safe_execute(func, series_raw, series_typed, base_metadata, check_id, col)

                # Aplicar severity override si existe en config
                if result and not result.passed:
                    override = ConfigLoader.get_severity_override(self.config, check_id)
                    if override:
                        result.severity = override

                results.append(result)

        # ── Checks a nivel dataset ──
        results.extend(self._run_dataset_checks(df, df_raw, column_types, date_col))

        return results

    def _run_dataset_checks(self, df, df_raw, column_types, date_col):
        """Ejecuta checks que operan a nivel dataset (cross-column, null patterns, etc.)."""
        results = []

        # Cross-column correlation analysis
        try:
            from checks.cross_column_checks import run_cross_column_checks
            results.extend(run_cross_column_checks(df, df_raw, column_types))
        except Exception as e:
            logger.warning("Error en análisis cross-column: %s: %s", type(e).__name__, str(e)[:200])
            results.append(CheckResult(
                check_id="CROSS_COLUMN", column="__dataset__", passed=True, severity="INFO",
                value=0.0, threshold=0.0,
                message=f"Error en análisis cross-column: {type(e).__name__}: {str(e)[:200]}",
                metadata={"error": True},
            ))

        # Null pattern analysis
        try:
            from checks.null_pattern_checks import run_null_pattern_checks
            results.extend(run_null_pattern_checks(df, df_raw))
        except Exception as e:
            logger.warning("Error en análisis de nulidad: %s: %s", type(e).__name__, str(e)[:200])
            results.append(CheckResult(
                check_id="NULL_PATTERNS", column="__dataset__", passed=True, severity="INFO",
                value=0.0, threshold=0.0,
                message=f"Error en análisis de nulidad: {type(e).__name__}: {str(e)[:200]}",
                metadata={"error": True},
            ))

        # Time series checks
        try:
            from checks.timeseries_checks import run_timeseries_checks
            results.extend(run_timeseries_checks(df, df_raw, column_types, date_col=date_col))
        except Exception as e:
            logger.warning("Error en análisis temporal: %s: %s", type(e).__name__, str(e)[:200])
            results.append(CheckResult(
                check_id="TIMESERIES", column="__dataset__", passed=True, severity="INFO",
                value=0.0, threshold=0.0,
                message=f"Error en análisis temporal: {type(e).__name__}: {str(e)[:200]}",
                metadata={"error": True},
            ))

        # PII detection
        try:
            from checks.pii_checks import run_pii_checks
            results.extend(run_pii_checks(df_raw, df))
        except Exception as e:
            logger.warning("Error en detección PII: %s: %s", type(e).__name__, str(e)[:200])
            results.append(CheckResult(
                check_id="PII_DETECTION", column="__dataset__", passed=True, severity="INFO",
                value=0.0, threshold=0.0,
                message=f"Error en detección PII: {type(e).__name__}: {str(e)[:200]}",
                metadata={"error": True},
            ))

        # Temporal completeness
        try:
            from checks.temporal_completeness_checks import run_temporal_completeness_checks
            results.extend(run_temporal_completeness_checks(df, df_raw, column_types, date_col=date_col))
        except Exception as e:
            logger.warning("Error en completitud temporal: %s: %s", type(e).__name__, str(e)[:200])
            results.append(CheckResult(
                check_id="TEMPORAL_COMPLETENESS", column="__dataset__", passed=True, severity="INFO",
                value=0.0, threshold=0.0,
                message=f"Error en completitud temporal: {type(e).__name__}: {str(e)[:200]}",
                metadata={"error": True},
            ))

        return results

    def _safe_execute(
        self,
        func,
        series_raw: pd.Series,
        series_typed: pd.Series,
        metadata: dict,
        check_id: str,
        column: str,
    ) -> CheckResult:
        """Ejecuta un check dentro de try/except. Errores generan INFO, nunca crash."""
        try:
            return func(series_raw, series_typed, metadata)
        except Exception as e:
            logger.warning("Check %s falló en columna '%s': %s: %s",
                           check_id, column, type(e).__name__, str(e)[:200])
            return CheckResult(
                check_id=check_id,
                column=column,
                passed=True,
                severity="INFO",
                value=0.0,
                threshold=0.0,
                message=f"Error al ejecutar check: {type(e).__name__}: {str(e)[:200]}",
                metadata={"error": True, "error_type": type(e).__name__},
            )
