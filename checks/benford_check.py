"""
Ley de Benford: verifica si la distribución de primeros dígitos sigue la ley de Benford.

Útil para detectar fraude en datos financieros/contables.
"""

import numpy as np
import pandas as pd
from scipy import stats

from models.check_result import CheckResult


# Distribución esperada de Benford
BENFORD_EXPECTED = {d: np.log10(1 + 1/d) for d in range(1, 10)}


def check_benford_law(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """BENFORD_LAW: test Chi² de la distribución de primeros dígitos vs Benford."""
    s = pd.to_numeric(series_typed, errors="coerce").dropna()
    s = s[s.abs() > 0]  # Excluir ceros

    if len(s) < 100:
        return CheckResult(
            check_id="BENFORD_LAW", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Datos insuficientes para test de Benford (se requieren >100 valores no cero)",
        )

    # Extraer primer dígito significativo
    first_digits = s.abs().apply(lambda x: int(str(f"{x:.10e}")[0]) if x != 0 else 0)
    first_digits = first_digits[first_digits.between(1, 9)]

    if len(first_digits) < 100:
        return CheckResult(
            check_id="BENFORD_LAW", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0,
            message="Dígitos significativos insuficientes para test de Benford",
        )

    # Contar frecuencias observadas
    observed_counts = first_digits.value_counts().reindex(range(1, 10), fill_value=0)
    n = len(first_digits)

    # Frecuencias esperadas (Benford)
    expected_counts = np.array([BENFORD_EXPECTED[d] * n for d in range(1, 10)])
    observed = observed_counts.values.astype(float)

    # Chi-squared test
    chi2, p_value = stats.chisquare(observed, expected_counts)

    # MAD (Mean Absolute Deviation) — Nigrini's criterion
    observed_pcts = observed / n
    expected_pcts = np.array([BENFORD_EXPECTED[d] for d in range(1, 10)])
    mad = np.mean(np.abs(observed_pcts - expected_pcts))

    # MAD thresholds (Nigrini, 2012):
    # <0.006: close conformity
    # 0.006-0.012: acceptable conformity
    # 0.012-0.015: marginally acceptable
    # >0.015: nonconformity
    if mad > 0.015:
        conformity = "no conforme"
        severity = "MEDIUM"
    elif mad > 0.012:
        conformity = "marginalmente conforme"
        severity = "LOW"
    elif mad > 0.006:
        conformity = "conforme aceptable"
        severity = "PASS"
    else:
        conformity = "altamente conforme"
        severity = "PASS"

    digit_details = {}
    for d in range(1, 10):
        digit_details[str(d)] = {
            "observed_pct": round(float(observed_pcts[d - 1]), 4),
            "expected_pct": round(float(expected_pcts[d - 1]), 4),
            "deviation": round(float(observed_pcts[d - 1] - expected_pcts[d - 1]), 4),
        }

    return CheckResult(
        check_id="BENFORD_LAW", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(float(mad), 6), threshold=0.015,
        message=f"Benford's Law: MAD={mad:.4f} ({conformity}), χ²={chi2:.2f}, p={p_value:.4f}",
        metadata={
            "chi2": round(float(chi2), 4),
            "p_value": round(float(p_value), 6),
            "mad": round(float(mad), 6),
            "conformity": conformity,
            "digit_distribution": digit_details,
            "n_values_tested": int(n),
        },
    )


BENFORD_CHECKS = [
    {"check_id": "BENFORD_LAW", "function": check_benford_law},
]
