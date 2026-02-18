"""
PII Detection Checks — detecta datos personales sensibles (emails, phones, credit cards, SSN, CURP, RFC, IPs).
"""

import re
from typing import List

import pandas as pd
import numpy as np

from models.check_result import CheckResult

# ── Patrones de PII ──
PII_PATTERNS = {
    "CREDIT_CARD": {
        "pattern": re.compile(r"\b(?:4\d{3}|5[1-5]\d{2}|3[47]\d{2}|6(?:011|5\d{2}))[- ]?\d{4}[- ]?\d{4}[- ]?\d{1,4}\b"),
        "description": "Número de tarjeta de crédito",
        "severity": "CRITICAL",
    },
    "SSN_US": {
        "pattern": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        "description": "Social Security Number (US)",
        "severity": "CRITICAL",
    },
    "CURP_MX": {
        "pattern": re.compile(r"\b[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b", re.IGNORECASE),
        "description": "CURP (México)",
        "severity": "CRITICAL",
    },
    "RFC_MX": {
        "pattern": re.compile(r"\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b", re.IGNORECASE),
        "description": "RFC (México)",
        "severity": "HIGH",
    },
    "EMAIL": {
        "pattern": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
        "description": "Dirección de email",
        "severity": "HIGH",
    },
    "PHONE_INTL": {
        "pattern": re.compile(r"\b\+?\d{1,3}[-.\s]?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b"),
        "description": "Número de teléfono",
        "severity": "MEDIUM",
    },
    "IP_ADDRESS": {
        "pattern": re.compile(r"\b(?:25[0-5]|2[0-4]\d|[01]?\d{1,2})(?:\.(?:25[0-5]|2[0-4]\d|[01]?\d{1,2})){3}\b"),
        "description": "Dirección IP",
        "severity": "MEDIUM",
    },
    "IBAN": {
        "pattern": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b"),
        "description": "Número IBAN",
        "severity": "HIGH",
    },
}

# Umbral mínimo: al menos este % de filas deben coincidir para reportar
MIN_MATCH_PCT = 0.05  # 5%


def run_pii_checks(df_raw: pd.DataFrame, df: pd.DataFrame) -> List[CheckResult]:
    """Ejecuta detección de PII sobre todas las columnas de texto."""
    results = []
    n_rows = len(df_raw)
    if n_rows == 0:
        return results

    for col in df_raw.columns:
        series = df_raw[col].dropna().astype(str)
        if len(series) == 0:
            continue

        # Solo analizar columnas que parezcan texto (no puramente numérico)
        sample = series.head(100)
        numeric_pct = sample.apply(lambda x: x.replace(".", "").replace("-", "").replace(",", "").isdigit()).mean()

        for pii_type, pii_info in PII_PATTERNS.items():
            # Skip phone/email check si la columna ya se detectó como ese tipo semántico
            # (el TypeDetector ya lo maneja, aquí buscamos PII "escondida")
            pattern = pii_info["pattern"]

            # Para tarjetas de crédito, solo buscar en texto no puramente numérico
            # o en columnas numéricas largas
            matches = series.apply(lambda x: bool(pattern.search(str(x))))
            match_count = int(matches.sum())
            match_pct = match_count / n_rows

            if match_count > 0 and match_pct >= MIN_MATCH_PCT:
                # Obtener muestras enmascaradas
                matched_vals = series[matches].head(3).tolist()
                masked = [_mask_value(v) for v in matched_vals]

                results.append(CheckResult(
                    check_id="PII_DETECTED",
                    column=col,
                    passed=False,
                    severity=pii_info["severity"],
                    value=round(match_pct, 4),
                    threshold=MIN_MATCH_PCT,
                    message=f"PII detectado: {pii_info['description']} — "
                            f"{match_count:,} registros ({match_pct:.1%})",
                    affected_count=match_count,
                    affected_pct=match_pct,
                    sample_values=masked,
                    metadata={
                        "pii_type": pii_type,
                        "description": pii_info["description"],
                        "match_count": match_count,
                    },
                ))

    return results


def _mask_value(value: str) -> str:
    """Enmascara un valor PII mostrando solo inicio y fin."""
    if len(value) <= 4:
        return "****"
    show = max(2, len(value) // 4)
    return value[:show] + "*" * (len(value) - show * 2) + value[-show:]
