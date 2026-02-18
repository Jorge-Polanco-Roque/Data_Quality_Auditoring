#!/usr/bin/env python3
"""Genera un reporte Markdown dinamico y amigable para ejecutivos a partir del JSON producido por data_quality_auditor."""

import json
import sys
import argparse
from datetime import datetime

from core.check_descriptions import (
    SEVERITY_EMOJI, SEVERITY_LABEL, SEVERITY_LABEL_SHORT,
    GRADE_EMOJI, GRADE_LABEL, SEMANTIC_TYPE_LABEL, STAT_LABEL,
    friendly_title, business_impact, friendly_type, friendly_severity,
    severity_short,
)


def load_report(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fmt_type(semantic_type: str) -> str:
    """Traduce tipo semantico a etiqueta amigable."""
    return friendly_type(semantic_type)


def _fmt_sev(severity: str) -> str:
    """Devuelve emoji + etiqueta amigable para severidad."""
    emoji = SEVERITY_EMOJI.get(severity, "")
    return f"{emoji} {friendly_severity(severity)}"


def _fmt_sev_short(severity: str) -> str:
    """Devuelve emoji + nombre corto para tablas."""
    emoji = SEVERITY_EMOJI.get(severity, "")
    return f"{emoji} {severity_short(severity)}"


def _explain_check(check_id: str) -> str:
    """Devuelve explicacion de negocio para un check_id."""
    return business_impact(check_id)


def section_header(report: dict) -> str:
    meta = report["report_metadata"]
    summary = report["dataset_summary"]
    grade = summary["health_grade"]
    score = summary["health_score"]
    grade_desc = GRADE_LABEL.get(grade, "")

    lines = [
        f"# Reporte de Calidad de Datos",
        "",
        f"**Archivo analizado:** `{meta['file_analyzed']}`  ",
        f"**Fecha del analisis:** {meta['generated_at'][:10]}  ",
        f"**Filas:** {meta['total_rows']:,} | **Columnas:** {meta['total_columns']}",
        "",
        "---",
        "",
        "## ¿Como leer este reporte?",
        "",
        "Este reporte evalua automaticamente la calidad de su archivo de datos. Cada columna",
        "recibe una calificacion de 0 a 100, y el archivo completo obtiene un grado general.",
        "Los problemas encontrados se clasifican por gravedad para priorizar las acciones.",
        "",
        "| Nivel de alerta | Significado | ¿Que hacer? |",
        "|-----------------|-------------|-------------|",
        "| 🔴 **Critico** | Problema grave que invalida los datos | Resolver antes de usar los datos |",
        "| 🟠 **Alto** | Problema importante que distorsiona resultados | Investigar y corregir pronto |",
        "| 🟡 **Medio** | Problema moderado que conviene atender | Revisar y planificar correccion |",
        "| 🟢 **Bajo** | Detalle menor, no urgente | Documentar y monitorear |",
        "| 🔵 **Informativo** | Observacion, no es un problema | Solo para conocimiento |",
        "",
        "---",
        "",
        "## Calificacion General",
        "",
        f"### {GRADE_EMOJI.get(grade, '')} Puntaje: **{score}/100** — Grado: **{grade}**",
        "",
        f"> **{grade_desc}**",
        "",
        f"| Dato | Valor |",
        f"|------|-------|",
        f"| Filas analizadas | {meta['total_rows']:,} |",
        f"| Columnas analizadas | {meta['total_columns']} |",
        f"| Codificacion del archivo | `{meta['encoding']}` |",
        "",
    ]
    return "\n".join(lines)


def section_severity_summary(report: dict) -> str:
    sev = report["dataset_summary"]["issues_by_severity"]
    total = report["dataset_summary"]["total_issues"]

    sev_desc = {
        "CRITICAL": "Requieren accion inmediata",
        "HIGH": "Investigar a la brevedad",
        "MEDIUM": "Planificar correccion",
        "LOW": "Monitorear, no urgentes",
        "INFO": "Solo informativo, sin accion requerida",
    }

    lines = [
        "## Resumen de problemas encontrados",
        "",
        f"Se encontraron **{total} hallazgos** en total, distribuidos asi:",
        "",
        "| Nivel | Cantidad | ¿Que significa? |",
        "|:------|:--------:|:----------------|",
    ]

    for level in ("CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"):
        count = sev.get(level, 0)
        emoji = SEVERITY_EMOJI.get(level, "")
        label = severity_short(level)
        desc = sev_desc.get(level, "")
        lines.append(f"| {emoji} **{label}** | {count} | {desc} |")
    lines.append(f"| | **{total}** | **Total de hallazgos** |")
    lines.append("")
    return "\n".join(lines)


def section_column_health(report: dict) -> str:
    profiles = report.get("column_profiles", {})
    if not profiles:
        return ""

    sorted_cols = sorted(profiles.items(), key=lambda x: x[1].get("health_score", 100))

    lines = [
        "## Estado de cada columna",
        "",
        "Ordenado de peor a mejor calificacion:",
        "",
        "| Columna | Tipo de dato | Puntaje | Grado | % Vacios | Valores distintos | Problemas |",
        "|---------|-------------|---------|-------|----------|-------------------|-----------|",
    ]

    for col_name, profile in sorted_cols:
        grade = profile.get("health_grade", "?")
        emoji = GRADE_EMOJI.get(grade, "")
        null_pct = profile.get("null_pct", 0)
        issues_count = profile.get("checks_failed", 0)
        sem_type = _fmt_type(profile.get("semantic_type", "?"))
        lines.append(
            f"| `{col_name}` "
            f"| {sem_type} "
            f"| {profile.get('health_score', 0)}/100 "
            f"| {emoji} {grade} "
            f"| {null_pct:.1%} "
            f"| {profile.get('n_unique', 0):,} "
            f"| {issues_count} |"
        )

    lines.append("")
    return "\n".join(lines)


def section_critical_bullets(report: dict) -> str:
    """Resumen ejecutivo de los hallazgos mas criticos en bullets concisos."""
    issues = report.get("critical_issues", [])
    summary = report.get("dataset_summary", {})
    profiles = report.get("column_profiles", {})

    if not issues and summary.get("total_issues", 0) == 0:
        return ""

    lines = [
        "## Lo mas importante",
        "",
    ]

    # Score general
    score = summary.get("health_score", 0)
    grade = summary.get("health_grade", "?")
    total = summary.get("total_issues", 0)
    sev = summary.get("issues_by_severity", {})
    lines.append(f"- **Calificacion general: {score}/100 ({grade})** — {total} hallazgos en total")

    # Severidades alarmantes
    crit = sev.get("CRITICAL", 0)
    high = sev.get("HIGH", 0)
    if crit > 0 or high > 0:
        parts = []
        if crit > 0:
            parts.append(f"**{crit} critico(s)**")
        if high > 0:
            parts.append(f"**{high} alto(s)**")
        lines.append(f"- Se encontraron {' y '.join(parts)} que requieren atencion inmediata")

    # Bullets por columna agrupados
    cols_seen = {}
    for issue in issues:
        col = issue.get("column", "?")
        check = issue.get("check_id", "?")
        sev_level = issue.get("severity", "?")
        affected = issue.get("affected_count", 0)
        pct = issue.get("affected_pct", 0)
        msg = issue.get("message", "")

        if col not in cols_seen:
            cols_seen[col] = []
        cols_seen[col].append((check, sev_level, affected, pct, msg))

    for col, col_issues in cols_seen.items():
        emoji = SEVERITY_EMOJI.get(col_issues[0][1], "")
        if len(col_issues) == 1:
            check, sev_level, affected, pct, msg = col_issues[0]
            explanation = _explain_check(check)
            lines.append(f"- {emoji} **`{col}`**: {explanation if explanation else msg}")
        else:
            total_affected = sum(c[2] for c in col_issues)
            # Usar la explicacion del check mas severo
            main_check = col_issues[0][0]
            explanation = _explain_check(main_check)
            lines.append(f"- {emoji} **`{col}`**: {len(col_issues)} problemas detectados que afectan ~{total_affected:,} registros. {explanation if explanation else ''}")

    # Columnas mas deterioradas
    worst = [
        (name, p.get("health_score", 100), p.get("health_grade", "?"))
        for name, p in profiles.items()
        if p.get("health_grade", "A") in ("D", "F") and name != "__dataset__"
    ]
    if worst:
        worst.sort(key=lambda x: x[1])
        worst_str = ", ".join(f"`{w[0]}` ({w[1]}/100)" for w in worst[:5])
        lines.append(f"- **Columnas que necesitan atencion urgente:** {worst_str}")

    lines.append("")
    return "\n".join(lines)


def section_critical_issues(report: dict) -> str:
    issues = report.get("critical_issues", [])
    if not issues:
        return "## Hallazgos que requieren atencion\n\n✅ No se encontraron problemas criticos ni altos. Los datos estan en buen estado.\n\n"

    lines = [
        "## Hallazgos que requieren atencion",
        "",
        "A continuacion se detallan los problemas mas graves encontrados. Cada uno incluye",
        "una explicacion de por que importa y ejemplos de los datos afectados.",
        "",
    ]

    for issue in issues:
        sev = issue.get("severity", "?")
        emoji = SEVERITY_EMOJI.get(sev, "")
        check_id = issue.get("check_id", "?")
        col = issue.get("column", "?")
        msg = issue.get("message", "")
        title = friendly_title(check_id)

        lines.append(f"### {emoji} `{col}` — {title}")
        lines.append("")
        lines.append(f"**Problema:** {msg}")
        lines.append(f"  ")
        lines.append(f"**Nivel de alerta:** {_fmt_sev(sev)}")
        lines.append(f"  ")
        lines.append(f"**Registros afectados:** {issue.get('affected_count', 0):,} ({issue.get('affected_pct', 0):.1%} del total)")

        # Explicacion de negocio
        explanation = business_impact(check_id)
        if explanation:
            lines.append(f"  ")
            lines.append(f"**¿Por que importa?** {explanation}")

        samples = issue.get("sample_values", [])
        if samples:
            sample_str = ", ".join(f"`{s}`" for s in samples[:5])
            lines.append(f"  ")
            lines.append(f"**Ejemplos encontrados:** {sample_str}")

        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def section_column_detail(report: dict) -> str:
    profiles = report.get("column_profiles", {})
    stats = report.get("statistical_summary", {})

    lines = [
        "## Detalle por columna",
        "",
        "Analisis individual de cada columna del archivo.",
        "",
    ]

    for col_name, profile in profiles.items():
        grade = profile.get("health_grade", "?")
        emoji = GRADE_EMOJI.get(grade, "")
        score = profile.get("health_score", 0)
        sem_type = _fmt_type(profile.get("semantic_type", "?"))
        grade_desc = GRADE_LABEL.get(grade, "")

        lines.append(f"### {emoji} `{col_name}` — {score}/100 ({grade})")
        lines.append("")
        lines.append(f"| Dato | Valor |")
        lines.append(f"|------|-------|")
        lines.append(f"| Tipo de dato detectado | {sem_type} |")
        lines.append(f"| % de celdas vacias | {profile.get('null_pct', 0):.1%} |")
        lines.append(f"| Valores distintos | {profile.get('n_unique', 0):,} |")
        lines.append(f"| Revisiones ejecutadas | {profile.get('checks_run', 0)} |")
        lines.append(f"| Problemas encontrados | {profile.get('checks_failed', 0)} |")

        # Estadisticas numericas
        num_stats = stats.get("numeric_columns", {}).get(col_name)
        if num_stats:
            lines.append("")
            lines.append("**Estadisticas numericas:**")
            lines.append("")
            lines.append("| Medida | Valor |")
            lines.append("|--------|-------|")
            for key in ("mean", "median", "std", "min", "max", "skewness", "kurtosis"):
                if key in num_stats:
                    label = STAT_LABEL.get(key, key)
                    lines.append(f"| {label} | {num_stats[key]:,.4f} |")
            if "outlier_count_iqr" in num_stats:
                lines.append(f"| {STAT_LABEL.get('outliers_IQR', 'Atipicos IQR')} | {num_stats['outlier_count_iqr']} |")
            if "outlier_count_zscore" in num_stats:
                lines.append(f"| {STAT_LABEL.get('outliers_Z', 'Atipicos Z')} | {num_stats['outlier_count_zscore']} |")

        # Estadisticas categoricas
        cat_stats = stats.get("categorical_columns", {}).get(col_name)
        if cat_stats:
            lines.append("")
            lines.append("**Estadisticas de categorias:**")
            lines.append("")
            lines.append(f"- Valor mas frecuente: `{cat_stats.get('top_value', '?')}` ({cat_stats.get('top_freq', 0):.1%} de los registros)")
            rare = cat_stats.get("rare_categories", [])
            if rare:
                lines.append(f"- Categorias con muy pocos registros: {', '.join(f'`{r}`' for r in rare[:5])}")

        # Estadisticas de fechas
        date_stats = stats.get("date_columns", {}).get(col_name)
        if date_stats:
            lines.append("")
            lines.append("**Estadisticas de fechas:**")
            lines.append("")
            lines.append(f"- Periodo cubierto: `{date_stats.get('min_date', '?')}` a `{date_stats.get('max_date', '?')}`")
            lines.append(f"- Huecos en la secuencia: {date_stats.get('gap_count', 0)}")

        # Issues
        issues = profile.get("issues", [])
        if issues:
            lines.append("")
            lines.append("**Problemas detectados:**")
            lines.append("")
            for iss in issues:
                check_id = iss.get("check_id", "?")
                sev_label = _fmt_sev_short(iss.get("severity", ""))
                title = friendly_title(check_id)
                msg = iss.get("message", "")
                explanation = business_impact(check_id)
                lines.append(f"- {sev_label} **{title}**: {msg}")
                if explanation:
                    lines.append(f"  *{explanation}*")

        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def section_recommendations(report: dict) -> str:
    recs = report.get("recommendations", [])
    if not recs:
        return "## Acciones recomendadas\n\n✅ No se requieren acciones — el archivo esta en buen estado.\n\n"

    lines = [
        "## Acciones recomendadas",
        "",
        "Lista priorizada de acciones para mejorar la calidad de los datos.",
        "Las acciones mas urgentes aparecen primero.",
        "",
        "| # | Columna | ¿Que hacer? | Urgencia |",
        "|---|---------|-------------|----------|",
    ]

    for rec in recs:
        emoji = SEVERITY_EMOJI.get(rec.get("estimated_impact", ""), "")
        sev_label = {
            "CRITICAL": "Inmediata",
            "HIGH": "Alta",
            "MEDIUM": "Media",
            "LOW": "Baja",
            "INFO": "Informativo",
        }.get(rec.get("estimated_impact", ""), "?")
        lines.append(
            f"| {rec['priority']} "
            f"| `{rec['column']}` "
            f"| {rec['action']} "
            f"| {emoji} {sev_label} |"
        )

    lines.append("")
    return "\n".join(lines)


def section_clean_vs_dirty(report: dict) -> str:
    summary = report["dataset_summary"]
    clean = summary.get("clean_columns", [])
    critical = summary.get("critical_columns", [])

    lines = [
        "## Mapa rapido de calidad",
        "",
    ]

    if clean:
        lines.append(f"**✅ Columnas sin problemas ({len(clean)}):** {', '.join(f'`{c}`' for c in clean)}")
    else:
        lines.append("**✅ Columnas sin problemas:** ninguna")

    lines.append("")

    if critical:
        lines.append(f"**🔴 Columnas con problemas graves ({len(critical)}):** {', '.join(f'`{c}`' for c in critical)}")
    else:
        lines.append("**🔴 Columnas con problemas graves:** ninguna")

    lines.append("")
    return "\n".join(lines)


def generate_markdown(report: dict) -> str:
    sections = [
        section_header(report),
        section_severity_summary(report),
        section_critical_bullets(report),
        section_clean_vs_dirty(report),
        section_column_health(report),
        section_critical_issues(report),
        section_column_detail(report),
        section_recommendations(report),
        "---",
        "",
        "*Reporte generado automaticamente por Data Quality Auditor.*",
        "",
    ]
    return "\n".join(sections)


def main():
    parser = argparse.ArgumentParser(description="Genera reporte Markdown desde JSON de auditoria")
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
