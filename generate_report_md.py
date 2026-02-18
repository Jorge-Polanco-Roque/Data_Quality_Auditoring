#!/usr/bin/env python3
"""Genera un reporte Markdown dinámico a partir del JSON producido por data_quality_auditor."""

import json
import sys
import argparse
from datetime import datetime


SEVERITY_EMOJI = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "MEDIUM": "🟡",
    "LOW": "🟢",
    "INFO": "🔵",
    "PASS": "✅",
}

GRADE_EMOJI = {
    "A": "🟢",
    "B": "🔵",
    "C": "🟡",
    "D": "🟠",
    "F": "🔴",
}


def load_report(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def section_header(report: dict) -> str:
    meta = report["report_metadata"]
    summary = report["dataset_summary"]
    grade = summary["health_grade"]
    score = summary["health_score"]

    lines = [
        f"# 📊 Data Quality Report — `{meta['file_analyzed']}`",
        "",
        f"> Generado automáticamente el **{meta['generated_at'][:10]}** por `data_quality_auditor`",
        "",
        "---",
        "",
        "## Resumen Ejecutivo",
        "",
        f"| Métrica | Valor |",
        f"|---------|-------|",
        f"| **Health Score** | {GRADE_EMOJI.get(grade, '')} **{score}/100 ({grade})** |",
        f"| Filas | {meta['total_rows']:,} |",
        f"| Columnas | {meta['total_columns']} |",
        f"| Encoding | `{meta['encoding']}` |",
        f"| Delimiter | `{meta['delimiter']}` |",
        "",
    ]
    return "\n".join(lines)


def section_severity_summary(report: dict) -> str:
    sev = report["dataset_summary"]["issues_by_severity"]
    total = report["dataset_summary"]["total_issues"]

    lines = [
        "## Distribución de Issues",
        "",
        "| Severidad | Cantidad | Indicador |",
        "|-----------|----------|-----------|",
    ]
    for level in ("CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"):
        count = sev.get(level, 0)
        bar = "█" * count + "░" * max(0, 10 - count) if count <= 10 else "█" * 10 + f" +{count - 10}"
        lines.append(f"| {SEVERITY_EMOJI.get(level, '')} **{level}** | {count} | `{bar}` |")
    lines.append(f"| **TOTAL** | **{total}** | |")
    lines.append("")
    return "\n".join(lines)


def section_column_health(report: dict) -> str:
    profiles = report.get("column_profiles", {})
    if not profiles:
        return ""

    # Ordenar por score ascendente (peores primero)
    sorted_cols = sorted(profiles.items(), key=lambda x: x[1].get("health_score", 100))

    lines = [
        "## Salud por Columna",
        "",
        "| Columna | Tipo Semántico | Score | Grado | Nulls | Únicos | Issues |",
        "|---------|---------------|-------|-------|-------|--------|--------|",
    ]

    for col_name, profile in sorted_cols:
        grade = profile.get("health_grade", "?")
        emoji = GRADE_EMOJI.get(grade, "")
        null_pct = profile.get("null_pct", 0)
        issues_count = profile.get("checks_failed", 0)
        lines.append(
            f"| `{col_name}` "
            f"| {profile.get('semantic_type', '?')} "
            f"| {profile.get('health_score', 0)} "
            f"| {emoji} {grade} "
            f"| {null_pct:.1%} "
            f"| {profile.get('n_unique', 0):,} "
            f"| {issues_count} |"
        )

    lines.append("")
    return "\n".join(lines)


def section_critical_issues(report: dict) -> str:
    issues = report.get("critical_issues", [])
    if not issues:
        return "## Puntos Críticos\n\n✅ No se encontraron issues CRITICAL o HIGH.\n\n"

    lines = [
        "## ⚠️ Puntos Críticos",
        "",
        "Issues que requieren acción inmediata:",
        "",
    ]

    for issue in issues:
        sev = issue.get("severity", "?")
        emoji = SEVERITY_EMOJI.get(sev, "")
        lines.append(f"### {emoji} `{issue['column']}` → {issue['check_id']}")
        lines.append("")
        lines.append(f"- **Severidad:** {sev}")
        lines.append(f"- **Detalle:** {issue['message']}")
        lines.append(f"- **Afectados:** {issue.get('affected_count', 0):,} registros ({issue.get('affected_pct', 0):.1%})")

        samples = issue.get("sample_values", [])
        if samples:
            sample_str = ", ".join(f"`{s}`" for s in samples[:5])
            lines.append(f"- **Muestra:** {sample_str}")

        lines.append("")

    return "\n".join(lines)


def section_column_detail(report: dict) -> str:
    profiles = report.get("column_profiles", {})
    stats = report.get("statistical_summary", {})

    lines = [
        "## Detalle por Columna",
        "",
    ]

    for col_name, profile in profiles.items():
        grade = profile.get("health_grade", "?")
        emoji = GRADE_EMOJI.get(grade, "")
        lines.append(f"### {emoji} `{col_name}` — {profile.get('semantic_type', '?')} — Score: {profile.get('health_score', 0)}/100 ({grade})")
        lines.append("")

        # Info básica
        lines.append(f"- **dtype pandas:** `{profile.get('pandas_dtype', '?')}`")
        lines.append(f"- **Nulos:** {profile.get('null_pct', 0):.1%}")
        lines.append(f"- **Valores únicos:** {profile.get('n_unique', 0):,}")
        lines.append(f"- **Checks ejecutados:** {profile.get('checks_run', 0)} | **Fallidos:** {profile.get('checks_failed', 0)}")

        # Estadísticas numéricas si las hay
        num_stats = stats.get("numeric_columns", {}).get(col_name)
        if num_stats:
            lines.append("")
            lines.append("**Estadísticas numéricas:**")
            lines.append("")
            lines.append("| Stat | Valor |")
            lines.append("|------|-------|")
            for key in ("mean", "median", "std", "min", "max", "skewness", "kurtosis"):
                if key in num_stats:
                    lines.append(f"| {key} | {num_stats[key]:,.4f} |")
            if "outlier_count_iqr" in num_stats:
                lines.append(f"| outliers_IQR | {num_stats['outlier_count_iqr']} |")
            if "outlier_count_zscore" in num_stats:
                lines.append(f"| outliers_Z | {num_stats['outlier_count_zscore']} |")

        # Estadísticas categóricas si las hay
        cat_stats = stats.get("categorical_columns", {}).get(col_name)
        if cat_stats:
            lines.append("")
            lines.append("**Estadísticas categóricas:**")
            lines.append("")
            lines.append(f"- Valor más frecuente: `{cat_stats.get('top_value', '?')}` ({cat_stats.get('top_freq', 0):.1%})")
            rare = cat_stats.get("rare_categories", [])
            if rare:
                lines.append(f"- Categorías raras: {', '.join(f'`{r}`' for r in rare[:5])}")

        # Estadísticas de fechas si las hay
        date_stats = stats.get("date_columns", {}).get(col_name)
        if date_stats:
            lines.append("")
            lines.append("**Estadísticas temporales:**")
            lines.append("")
            lines.append(f"- Rango: `{date_stats.get('min_date', '?')}` → `{date_stats.get('max_date', '?')}`")
            lines.append(f"- Gaps detectados: {date_stats.get('gap_count', 0)}")

        # Issues de esta columna
        issues = profile.get("issues", [])
        if issues:
            lines.append("")
            lines.append("**Issues encontrados:**")
            lines.append("")
            for iss in issues:
                sev_emoji = SEVERITY_EMOJI.get(iss.get("severity", ""), "")
                lines.append(f"- {sev_emoji} **{iss['check_id']}** ({iss['severity']}): {iss['message']}")

        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def section_recommendations(report: dict) -> str:
    recs = report.get("recommendations", [])
    if not recs:
        return "## Recomendaciones\n\n✅ No hay recomendaciones — el dataset está limpio.\n\n"

    lines = [
        "## 📋 Recomendaciones Priorizadas",
        "",
        "| # | Categoría | Columna | Acción | Impacto |",
        "|---|-----------|---------|--------|---------|",
    ]

    for rec in recs:
        emoji = SEVERITY_EMOJI.get(rec.get("estimated_impact", ""), "")
        lines.append(
            f"| {rec['priority']} "
            f"| {rec['category']} "
            f"| `{rec['column']}` "
            f"| {rec['action']} "
            f"| {emoji} {rec.get('estimated_impact', '?')} |"
        )

    lines.append("")
    return "\n".join(lines)


def section_clean_vs_dirty(report: dict) -> str:
    summary = report["dataset_summary"]
    clean = summary.get("clean_columns", [])
    critical = summary.get("critical_columns", [])

    lines = [
        "## Mapa de Calidad",
        "",
    ]

    if clean:
        lines.append(f"**✅ Columnas limpias ({len(clean)}):** {', '.join(f'`{c}`' for c in clean)}")
    else:
        lines.append("**✅ Columnas limpias:** ninguna")

    lines.append("")

    if critical:
        lines.append(f"**🔴 Columnas críticas ({len(critical)}):** {', '.join(f'`{c}`' for c in critical)}")
    else:
        lines.append("**🔴 Columnas críticas:** ninguna")

    lines.append("")
    return "\n".join(lines)


def generate_markdown(report: dict) -> str:
    sections = [
        section_header(report),
        section_severity_summary(report),
        section_clean_vs_dirty(report),
        section_column_health(report),
        section_critical_issues(report),
        section_column_detail(report),
        section_recommendations(report),
        "---",
        "",
        f"*Reporte generado automáticamente por `generate_report_md.py` a partir del JSON de auditoría.*",
        "",
    ]
    return "\n".join(sections)


def main():
    parser = argparse.ArgumentParser(description="Genera reporte Markdown desde JSON de auditoría")
    parser.add_argument("--input", required=True, help="Ruta al JSON generado por data_quality_auditor")
    parser.add_argument("--output", required=True, help="Ruta para el archivo Markdown de salida")
    args = parser.parse_args()

    report = load_report(args.input)
    md = generate_markdown(report)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Reporte Markdown generado: {args.output}")


if __name__ == "__main__":
    main()
