#!/usr/bin/env python3
"""Genera un reporte HTML interactivo a partir del JSON de auditoría."""

import json
import argparse
from datetime import datetime


SEVERITY_COLORS = {
    "CRITICAL": "#e74c3c",
    "HIGH": "#e67e22",
    "MEDIUM": "#f1c40f",
    "LOW": "#27ae60",
    "INFO": "#3498db",
    "PASS": "#95a5a6",
}

GRADE_COLORS = {
    "A": "#27ae60",
    "B": "#3498db",
    "C": "#f1c40f",
    "D": "#e67e22",
    "F": "#e74c3c",
}


def generate_html(report: dict) -> str:
    """Genera HTML completo con charts embebidos usando Chart.js CDN."""
    meta = report["report_metadata"]
    summary = report["dataset_summary"]
    profiles = report.get("column_profiles", {})
    stats_summary = report.get("statistical_summary", {})
    grade = summary["health_grade"]
    score = summary["health_score"]

    severity_data = summary["issues_by_severity"]
    col_scores = [(name, p["health_score"], p["health_grade"]) for name, p in profiles.items()]

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Quality Report — {meta['file_analyzed']}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f5f6fa; color: #2c3e50; line-height: 1.6; }}
.container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
.header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
           color: white; padding: 30px; border-radius: 12px; margin-bottom: 20px; }}
.header h1 {{ font-size: 1.8em; margin-bottom: 5px; }}
.header .subtitle {{ opacity: 0.8; }}
.score-badge {{ display: inline-block; background: {GRADE_COLORS.get(grade, '#95a5a6')};
               color: white; padding: 8px 20px; border-radius: 20px; font-size: 1.4em;
               font-weight: bold; margin-top: 10px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 20px; margin-bottom: 20px; }}
.card {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); }}
.card h2 {{ font-size: 1.1em; margin-bottom: 15px; color: #34495e; border-bottom: 2px solid #eee; padding-bottom: 8px; }}
.stat-row {{ display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid #f0f0f0; }}
.stat-label {{ color: #7f8c8d; }}
.stat-value {{ font-weight: 600; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
th {{ background: #f8f9fa; text-align: left; padding: 10px 12px; font-weight: 600; color: #34495e; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
tr:hover td {{ background: #f8f9fa; }}
.severity {{ display: inline-block; padding: 2px 8px; border-radius: 10px; color: white; font-size: 0.85em; font-weight: 600; }}
.chart-container {{ position: relative; height: 250px; }}
.full-width {{ grid-column: 1 / -1; }}
.issue-item {{ padding: 10px; margin: 5px 0; border-left: 4px solid; border-radius: 4px; background: #f8f9fa; }}
.footer {{ text-align: center; color: #95a5a6; padding: 20px; font-size: 0.85em; }}
</style>
</head>
<body>
<div class="container">

<div class="header">
    <h1>Data Quality Report</h1>
    <div class="subtitle">{meta['file_analyzed']} &mdash; {meta['generated_at'][:10]}</div>
    <div class="score-badge">{score}/100 ({grade})</div>
</div>

<div class="grid">
    <div class="card">
        <h2>Resumen del Dataset</h2>
        <div class="stat-row"><span class="stat-label">Filas</span><span class="stat-value">{meta['total_rows']:,}</span></div>
        <div class="stat-row"><span class="stat-label">Columnas</span><span class="stat-value">{meta['total_columns']}</span></div>
        <div class="stat-row"><span class="stat-label">Encoding</span><span class="stat-value">{meta['encoding']}</span></div>
        <div class="stat-row"><span class="stat-label">Delimiter</span><span class="stat-value">{meta['delimiter']}</span></div>
        <div class="stat-row"><span class="stat-label">Total Issues</span><span class="stat-value">{summary['total_issues']}</span></div>
    </div>
    <div class="card">
        <h2>Distribución de Issues</h2>
        <div class="chart-container"><canvas id="severityChart"></canvas></div>
    </div>
</div>

<div class="grid">
    <div class="card full-width">
        <h2>Salud por Columna</h2>
        <div class="chart-container" style="height:300px"><canvas id="columnChart"></canvas></div>
    </div>
</div>

<div class="grid">
    <div class="card full-width">
        <h2>Detalle por Columna</h2>
        <table>
            <thead>
                <tr><th>Columna</th><th>Tipo</th><th>Score</th><th>Grado</th><th>Nulls</th><th>Únicos</th><th>Issues</th></tr>
            </thead>
            <tbody>
"""

    for col_name, profile in profiles.items():
        grade_c = profile.get("health_grade", "?")
        color = GRADE_COLORS.get(grade_c, "#95a5a6")
        issues_html = ""
        for iss in profile.get("issues", []):
            sev_color = SEVERITY_COLORS.get(iss["severity"], "#95a5a6")
            issues_html += (f'<div class="issue-item" style="border-color:{sev_color}">'
                           f'<span class="severity" style="background:{sev_color}">{iss["severity"]}</span> '
                           f'<strong>{iss["check_id"]}</strong>: {iss["message"]}</div>')

        html += f"""                <tr>
                    <td><strong>{col_name}</strong></td>
                    <td>{profile.get('semantic_type', '?')}</td>
                    <td>{profile.get('health_score', 0)}</td>
                    <td style="color:{color};font-weight:bold">{grade_c}</td>
                    <td>{profile.get('null_pct', 0):.1%}</td>
                    <td>{profile.get('n_unique', 0):,}</td>
                    <td>{profile.get('checks_failed', 0)}</td>
                </tr>
"""
        if issues_html:
            html += f'                <tr><td colspan="7" style="padding:0 12px 12px">{issues_html}</td></tr>\n'

    # Critical issues section
    critical_html = ""
    critical_issues = report.get("critical_issues", [])
    if critical_issues:
        critical_html = '<div class="grid"><div class="card full-width"><h2>Puntos Críticos</h2>'
        for iss in critical_issues:
            sev_color = SEVERITY_COLORS.get(iss["severity"], "#95a5a6")
            critical_html += (f'<div class="issue-item" style="border-color:{sev_color}">'
                             f'<span class="severity" style="background:{sev_color}">{iss["severity"]}</span> '
                             f'<strong>{iss["column"]}</strong> → {iss["check_id"]}: {iss["message"]}</div>')
        critical_html += '</div></div>'

    # Recommendations
    recs_html = ""
    recs = report.get("recommendations", [])
    if recs:
        recs_html = ('<div class="grid"><div class="card full-width"><h2>Recomendaciones</h2><table>'
                     '<thead><tr><th>#</th><th>Categoría</th><th>Columna</th><th>Acción</th><th>Impacto</th></tr></thead><tbody>')
        for rec in recs:
            sev_color = SEVERITY_COLORS.get(rec.get("estimated_impact", ""), "#95a5a6")
            recs_html += (f'<tr><td>{rec["priority"]}</td><td>{rec["category"]}</td>'
                         f'<td><strong>{rec["column"]}</strong></td><td>{rec["action"]}</td>'
                         f'<td><span class="severity" style="background:{sev_color}">'
                         f'{rec.get("estimated_impact", "?")}</span></td></tr>')
        recs_html += '</tbody></table></div></div>'

    # Column names and scores for chart
    col_names_js = json.dumps([name for name, _, _ in col_scores])
    col_scores_js = json.dumps([score for _, score, _ in col_scores])
    col_colors_js = json.dumps([GRADE_COLORS.get(g, "#95a5a6") for _, _, g in col_scores])

    sev_labels = json.dumps(["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"])
    sev_values = json.dumps([severity_data.get(s, 0) for s in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]])
    sev_colors = json.dumps([SEVERITY_COLORS[s] for s in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]])

    html += f"""            </tbody>
        </table>
    </div>
</div>

{critical_html}
{recs_html}

<div class="footer">
    Reporte generado automáticamente por <code>data_quality_auditor</code> &mdash; {meta['generated_at'][:10]}
</div>

</div>

<script>
new Chart(document.getElementById('severityChart'), {{
    type: 'doughnut',
    data: {{
        labels: {sev_labels},
        datasets: [{{ data: {sev_values}, backgroundColor: {sev_colors}, borderWidth: 0 }}]
    }},
    options: {{ responsive: true, maintainAspectRatio: false,
               plugins: {{ legend: {{ position: 'right' }} }} }}
}});

new Chart(document.getElementById('columnChart'), {{
    type: 'bar',
    data: {{
        labels: {col_names_js},
        datasets: [{{ label: 'Health Score', data: {col_scores_js},
                     backgroundColor: {col_colors_js}, borderWidth: 0, borderRadius: 6 }}]
    }},
    options: {{ responsive: true, maintainAspectRatio: false,
               scales: {{ y: {{ beginAtZero: true, max: 100 }} }},
               plugins: {{ legend: {{ display: false }} }} }}
}});
</script>
</body>
</html>"""

    return html


def main():
    parser = argparse.ArgumentParser(description="Genera reporte HTML desde JSON de auditoría")
    parser.add_argument("--input", required=True, help="Ruta al JSON")
    parser.add_argument("--output", required=True, help="Ruta para HTML")
    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        report = json.load(f)

    html = generate_html(report)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Reporte HTML generado: {args.output}")


if __name__ == "__main__":
    main()
