"""
Executive Summary Report — reporte condensado de 1 página para stakeholders no técnicos.
Incluye: semáforo general, top 3 problemas, acción recomendada.
"""

from typing import Dict, List


TRAFFIC_LIGHT = {
    "A": ("VERDE", "La calidad de datos es excelente. No se requieren acciones inmediatas."),
    "B": ("VERDE", "La calidad de datos es buena. Se recomienda revisar los issues menores encontrados."),
    "C": ("AMARILLO", "La calidad de datos es aceptable pero tiene problemas que deben atenderse."),
    "D": ("ROJO", "La calidad de datos es deficiente. Se requieren acciones correctivas urgentes."),
    "F": ("ROJO", "La calidad de datos es crítica. No se recomienda usar estos datos sin corrección previa."),
}


def generate_executive_summary(report: Dict) -> str:
    """Genera un resumen ejecutivo condensado."""
    meta = report["report_metadata"]
    summary = report["dataset_summary"]
    grade = summary["health_grade"]
    score = summary["health_score"]
    sev = summary["issues_by_severity"]

    light_color, light_msg = TRAFFIC_LIGHT.get(grade, ("GRIS", "Estado desconocido"))

    lines = [
        "# Resumen Ejecutivo — Calidad de Datos",
        "",
        f"**Archivo:** `{meta['file_analyzed']}`  ",
        f"**Fecha:** {meta['generated_at'][:10]}  ",
        f"**Filas:** {meta['total_rows']:,} | **Columnas:** {meta['total_columns']}",
        "",
        "---",
        "",
        "## Estado General",
        "",
        f"### Score: {score}/100 ({grade}) — Semáforo: {light_color}",
        "",
        f"> {light_msg}",
        "",
    ]

    # Resumen de severidades (solo si hay)
    active_sevs = [(k, v) for k, v in sev.items() if v > 0 and k != "INFO"]
    if active_sevs:
        lines.append("| Severidad | Cantidad |")
        lines.append("|-----------|----------|")
        for level, count in active_sevs:
            lines.append(f"| {level} | {count} |")
        lines.append("")

    # Top 3 problemas
    critical_issues = report.get("critical_issues", [])
    recs = report.get("recommendations", [])

    lines.append("## Top Problemas")
    lines.append("")

    if not critical_issues and sev.get("CRITICAL", 0) == 0 and sev.get("HIGH", 0) == 0:
        lines.append("No se encontraron problemas críticos o de alta severidad.")
        lines.append("")
    else:
        for i, issue in enumerate(critical_issues[:3], 1):
            lines.append(f"**{i}. [{issue['severity']}] {issue['column']}** — {issue['check_id']}")
            lines.append(f"   {issue['message'][:150]}")
            lines.append("")

    # Acciones recomendadas (top 3)
    lines.append("## Acciones Recomendadas")
    lines.append("")

    if not recs:
        lines.append("No se requieren acciones correctivas.")
    else:
        for i, rec in enumerate(recs[:3], 1):
            lines.append(f"{i}. **{rec['column']}:** {rec['action']}")
        lines.append("")

    # Columnas que requieren atención
    critical_cols = summary.get("critical_columns", [])
    clean_cols = summary.get("clean_columns", [])

    lines.append("## Mapa Rápido")
    lines.append("")

    if clean_cols:
        lines.append(f"**Columnas OK ({len(clean_cols)}):** {', '.join(f'`{c}`' for c in clean_cols[:10])}")
    if critical_cols:
        lines.append(f"**Columnas Críticas ({len(critical_cols)}):** {', '.join(f'`{c}`' for c in critical_cols[:10])}")

    if not clean_cols and not critical_cols:
        lines.append("Todas las columnas tienen calidad intermedia.")

    lines.extend(["", "---", "",
                   "*Reporte ejecutivo generado automáticamente por Data Quality Auditor.*", ""])

    # Trend section if available
    trend = report.get("quality_trend")
    if trend and trend.get("previous_runs", 0) > 0:
        # Insert trend before the footer
        trend_lines = [
            "",
            "## Tendencia de Calidad",
            "",
            f"**Corridas anteriores:** {trend['previous_runs']}  ",
            f"**Tendencia:** {trend.get('trend_description', 'N/A')}  ",
        ]
        if "avg_previous_score" in trend:
            trend_lines.append(f"**Score promedio histórico:** {trend['avg_previous_score']}/100")
        trend_lines.append("")

        # Insert before the last 3 lines (---, footer, "")
        lines = lines[:-3] + trend_lines + lines[-3:]

    return "\n".join(lines)
