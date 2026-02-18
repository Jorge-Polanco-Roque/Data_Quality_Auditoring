import re
import pandas as pd
import numpy as np

from models.check_result import CheckResult


def check_rare_categories(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """RARE_CATEGORIES: categorías con frecuencia < 0.5% del total."""
    non_null = series_typed.dropna()
    if len(non_null) == 0:
        return CheckResult(
            check_id="RARE_CATEGORIES", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.005, message="Sin datos",
        )

    freq = non_null.value_counts(normalize=True)
    rare = freq[freq < 0.005]
    n_rare = len(rare)

    if n_rare == 0:
        severity = "PASS"
    elif n_rare > 10:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    samples = rare.head(5).index.tolist()
    total_rare_count = int(non_null.isin(rare.index).sum())
    pct = total_rare_count / len(non_null)

    return CheckResult(
        check_id="RARE_CATEGORIES", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=float(n_rare), threshold=0.005,
        message=f"{n_rare} categorías raras (frecuencia < 0.5%)",
        affected_count=total_rare_count, affected_pct=pct,
        sample_values=[str(s) for s in samples],
    )


def check_case_inconsistency(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """CASE_INCONSISTENCY: misma categoría con diferente capitalización."""
    non_null = series_raw.astype(str).str.strip()
    non_null = non_null[non_null != ""]
    if len(non_null) == 0:
        return CheckResult(
            check_id="CASE_INCONSISTENCY", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.0, message="Sin datos",
        )

    unique_vals = non_null.unique()
    lower_map = {}
    for val in unique_vals:
        key = val.lower()
        if key not in lower_map:
            lower_map[key] = []
        lower_map[key].append(val)

    inconsistent = {k: v for k, v in lower_map.items() if len(v) > 1}
    n_issues = len(inconsistent)

    if n_issues == 0:
        severity = "PASS"
    elif n_issues > 5:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    samples = []
    for k, variants in list(inconsistent.items())[:3]:
        samples.append(" / ".join(variants))

    return CheckResult(
        check_id="CASE_INCONSISTENCY", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=float(n_issues), threshold=0.0,
        message=f"{n_issues} grupo(s) con inconsistencia de capitalización",
        sample_values=samples,
        metadata={"inconsistent_groups": {k: v for k, v in list(inconsistent.items())[:5]}},
    )


def check_encoding_anomaly(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """ENCODING_ANOMALY: caracteres raros o de control en categorías."""
    non_null = series_raw.astype(str).str.strip()
    non_null = non_null[non_null != ""]

    control_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
    replacement_char = re.compile(r"[�\ufffd]")

    mask = non_null.apply(lambda x: bool(control_re.search(x) or replacement_char.search(x)))
    count = int(mask.sum())
    n = len(non_null)
    pct = count / n if n > 0 else 0.0

    if pct > 0.05:
        severity = "HIGH"
    elif count > 0:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    samples = non_null[mask].head(5).apply(repr).tolist()

    return CheckResult(
        check_id="ENCODING_ANOMALY", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(pct, 4), threshold=0.0,
        message=f"{count:,} valores con caracteres de control o encoding anómalo ({pct:.1%})",
        affected_count=count, affected_pct=pct, sample_values=samples,
    )


def check_class_imbalance(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """CLASS_IMBALANCE: una categoría representa >90-95% de los datos."""
    non_null = series_typed.dropna()
    if len(non_null) == 0:
        return CheckResult(
            check_id="CLASS_IMBALANCE", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.90, message="Sin datos",
        )

    freq = non_null.value_counts(normalize=True)
    top_pct = float(freq.iloc[0])
    top_val = freq.index[0]

    if top_pct >= 0.95:
        severity = "HIGH"
    elif top_pct >= 0.90:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    return CheckResult(
        check_id="CLASS_IMBALANCE", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=round(top_pct, 4), threshold=0.90,
        message=f"Categoría dominante '{top_val}' representa {top_pct:.1%}",
        metadata={"dominant_category": str(top_val), "dominant_pct": round(top_pct, 4)},
    )


def check_typo_candidates(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """TYPO_CANDIDATES: categorías similares por distancia de Levenshtein."""
    non_null = series_typed.dropna()
    if len(non_null) == 0:
        return CheckResult(
            check_id="TYPO_CANDIDATES", column=series_raw.name, passed=True,
            severity="PASS", value=0.0, threshold=0.85, message="Sin datos",
        )

    freq = non_null.value_counts()
    # Solo categorías con más de 1 ocurrencia
    cats = [str(c) for c in freq[freq > 1].index]

    # Limitar para performance
    if len(cats) > 200:
        cats = cats[:200]

    typo_pairs = []
    try:
        from rapidfuzz import fuzz
        for i, a in enumerate(cats):
            for b in cats[i + 1:]:
                sim = fuzz.ratio(a.lower(), b.lower()) / 100.0
                if sim >= 0.85 and a.lower() != b.lower():
                    typo_pairs.append((a, b, round(sim, 2)))
    except ImportError:
        return CheckResult(
            check_id="TYPO_CANDIDATES", column=series_raw.name, passed=True,
            severity="INFO", value=0.0, threshold=0.85,
            message="rapidfuzz no disponible para detección de typos",
        )

    n_pairs = len(typo_pairs)
    if n_pairs == 0:
        severity = "PASS"
    elif n_pairs > 5:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    samples = [f"'{a}' ↔ '{b}' (sim={s})" for a, b, s in typo_pairs[:5]]

    return CheckResult(
        check_id="TYPO_CANDIDATES", column=series_raw.name,
        passed=severity == "PASS", severity=severity,
        value=float(n_pairs), threshold=0.85,
        message=f"{n_pairs} par(es) de categorías candidatas a typo",
        sample_values=samples,
        metadata={"pairs": [(a, b, s) for a, b, s in typo_pairs[:10]]},
    )


def check_cardinality_change(series_raw: pd.Series, series_typed: pd.Series, metadata: dict) -> CheckResult:
    """CARDINALITY_CHANGE: informa la cardinalidad para monitoreo."""
    non_null = series_typed.dropna()
    n_unique = non_null.nunique()
    n = len(non_null)
    ratio = n_unique / n if n > 0 else 0.0

    return CheckResult(
        check_id="CARDINALITY_CHANGE", column=series_raw.name,
        passed=True, severity="INFO",
        value=float(n_unique), threshold=0.0,
        message=f"{n_unique} categorías únicas (ratio: {ratio:.2%})",
        metadata={"n_unique": n_unique, "cardinality_ratio": round(ratio, 4)},
    )


CATEGORICAL_CHECKS = [
    {"check_id": "RARE_CATEGORIES", "function": check_rare_categories},
    {"check_id": "CARDINALITY_CHANGE", "function": check_cardinality_change},
    {"check_id": "CASE_INCONSISTENCY", "function": check_case_inconsistency},
    {"check_id": "ENCODING_ANOMALY", "function": check_encoding_anomaly},
    {"check_id": "CLASS_IMBALANCE", "function": check_class_imbalance},
    {"check_id": "TYPO_CANDIDATES", "function": check_typo_candidates},
]
