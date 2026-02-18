"""
LangGraph Quality Report Agent — genera un reporte Markdown visual e interpretativo
a partir del JSON de auditoría de calidad de datos.

Enfoque híbrido:
  - Elementos visuales (mermaid, tablas, histogramas) → generados determinísticamente en Python
  - Narrativa e interpretación → generados por LLM (GPT-4o-mini)

Pipeline LangGraph:
  START → load_report → [analyze_overview ∥ analyze_columns ∥ analyze_issues] → assemble_report → END

Uso:
    python quality_report_agent.py --input resultado/report.json
    python quality_report_agent.py --input resultado/report.json --output mi_reporte.md
"""

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import TypedDict

from dotenv import load_dotenv
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════════
# Constantes visuales
# ═══════════════════════════════════════════════════════════════════════════

SEVERITY_BADGE = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "MEDIUM": "🟡",
    "LOW": "🔵",
    "INFO": "⚪",
}

GRADE_LIGHT = {
    "A": "🟢",
    "B": "🟢",
    "C": "🟡",
    "D": "🔴",
    "F": "🔴",
}

TYPE_BADGE = {
    "NUMERIC_CONTINUOUS": "🔢",
    "NUMERIC_DISCRETE": "🔢",
    "DATE": "📅",
    "DATETIME": "📅",
    "CATEGORICAL": "📝",
    "BOOLEAN": "✅",
    "EMAIL": "📧",
    "PHONE": "📞",
    "ID": "🆔",
    "TEXT": "📄",
    "MIXED": "❓",
    "EMPTY": "⬜",
    "CONSTANT": "🔒",
}

GRADE_COLOR = {
    "A": "#2ecc71",
    "B": "#27ae60",
    "C": "#f39c12",
    "D": "#e74c3c",
    "F": "#c0392b",
}

BAR_FULL = "▓"
BAR_EMPTY = "░"
BAR_WIDTH = 20


# ═══════════════════════════════════════════════════════════════════════════
# Funciones auxiliares para generar visuales determinísticos
# ═══════════════════════════════════════════════════════════════════════════

def _health_bar(score: float) -> str:
    """Barra visual de salud: ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░░░░ 80/100"""
    filled = round(score / 100 * BAR_WIDTH)
    return f"`{BAR_FULL * filled}{BAR_EMPTY * (BAR_WIDTH - filled)}` **{score}/100**"


def _severity_pie_mermaid(issues_by_severity: dict) -> str:
    """Genera diagrama mermaid pie de issues por severidad."""
    active = {k: v for k, v in issues_by_severity.items() if v > 0}
    if not active:
        return ""
    lines = ['```mermaid', 'pie title Distribución de Issues por Severidad']
    for sev, count in active.items():
        lines.append(f'    "{sev} ({count})" : {count}')
    lines.append('```')
    return "\n".join(lines)


def _column_health_map_mermaid(column_profiles: dict) -> str:
    """Genera mermaid graph con mapa de salud de columnas por colores."""
    healthy = []
    attention = []
    critical = []

    for col, prof in column_profiles.items():
        score = prof.get("health_score", 100)
        grade = prof.get("health_grade", "A")
        entry = (col, score, grade)
        if score >= 95:
            healthy.append(entry)
        elif score >= 80:
            attention.append(entry)
        else:
            critical.append(entry)

    lines = ['```mermaid', 'graph LR']

    if healthy:
        lines.append('    subgraph "🟢 Saludables"')
        for col, score, grade in healthy:
            safe_id = col.replace(" ", "_").replace("-", "_")
            lines.append(f'        {safe_id}["{col}<br/>{score}/100 ({grade})"]')
        lines.append('    end')

    if attention:
        lines.append('    subgraph "🟡 Requieren Atención"')
        for col, score, grade in attention:
            safe_id = col.replace(" ", "_").replace("-", "_")
            lines.append(f'        {safe_id}["{col}<br/>{score}/100 ({grade})"]')
        lines.append('    end')

    if critical:
        lines.append('    subgraph "🔴 Críticas"')
        for col, score, grade in critical:
            safe_id = col.replace(" ", "_").replace("-", "_")
            lines.append(f'        {safe_id}["{col}<br/>{score}/100 ({grade})"]')
        lines.append('    end')

    # Estilos
    for col, score, grade in healthy:
        safe_id = col.replace(" ", "_").replace("-", "_")
        lines.append(f'    style {safe_id} fill:#2ecc71,color:#fff')
    for col, score, grade in attention:
        safe_id = col.replace(" ", "_").replace("-", "_")
        lines.append(f'    style {safe_id} fill:#f39c12,color:#fff')
    for col, score, grade in critical:
        safe_id = col.replace(" ", "_").replace("-", "_")
        lines.append(f'    style {safe_id} fill:#e74c3c,color:#fff')

    lines.append('```')
    return "\n".join(lines)


def _histogram_text(histogram: list) -> str:
    """Genera histograma de texto a partir de datos de bins."""
    if not histogram:
        return ""
    max_count = max((b.get("count", 0) for b in histogram), default=1) or 1
    lines = ["```"]
    for b in histogram:
        count = b.get("count", 0)
        rng = b.get("range", "?")
        filled = round(count / max_count * BAR_WIDTH)
        bar = BAR_FULL * filled + BAR_EMPTY * (BAR_WIDTH - filled)
        lines.append(f"  {rng:>14s}  {bar}  {count:>4d}")
    lines.append("```")
    return "\n".join(lines)


def _correlation_mermaid(critical_issues: list) -> str:
    """Genera diagrama mermaid de correlaciones si hay HIGH_CORRELATION."""
    corr_issue = None
    for issue in critical_issues:
        if issue.get("check_id") == "HIGH_CORRELATION":
            corr_issue = issue
            break
    if not corr_issue:
        return ""

    pairs = corr_issue.get("metadata", {}).get("pairs", [])
    if not pairs:
        return ""

    # Extraer columnas únicas
    cols = set()
    for p in pairs:
        pair_str = p.get("pair", "")
        parts = [x.strip() for x in pair_str.split("×")]
        cols.update(parts)

    lines = ['```mermaid', 'graph LR']
    for p in pairs:
        pair_str = p.get("pair", "")
        r = p.get("pearson_r", 0)
        parts = [x.strip() for x in pair_str.split("×")]
        if len(parts) == 2:
            a = parts[0].replace(" ", "_").replace("-", "_")
            b = parts[1].replace(" ", "_").replace("-", "_")
            lines.append(f'    {a}["{parts[0]}"] -->|"r={r}"| {b}["{parts[1]}"]')

    # Colores por VIF si disponible
    vif_issue = None
    for issue in critical_issues:
        if issue.get("check_id") == "MULTICOLLINEARITY_VIF":
            vif_issue = issue
            break

    if vif_issue:
        vif_values = vif_issue.get("metadata", {}).get("vif_values", {})
        for col_name, vif in vif_values.items():
            safe_id = col_name.replace(" ", "_").replace("-", "_")
            if vif > 10:
                lines.append(f'    style {safe_id} fill:#e74c3c,color:#fff')
            elif vif > 5:
                lines.append(f'    style {safe_id} fill:#f39c12,color:#fff')
            else:
                lines.append(f'    style {safe_id} fill:#3498db,color:#fff')

    lines.append('```')
    return "\n".join(lines)


def _remediation_flowchart(recommendations: list) -> str:
    """Genera diagrama mermaid de flujo de remediación (top 5)."""
    top = recommendations[:5]
    if not top:
        return ""

    lines = ['```mermaid', 'graph TD']
    severity_colors = {
        "HIGH": "#e74c3c",
        "MEDIUM": "#f39c12",
        "LOW": "#3498db",
        "CRITICAL": "#c0392b",
    }

    for i, rec in enumerate(top):
        node_id = f"R{i+1}"
        col = rec.get("column", "?")
        action = rec.get("action", "?")
        # Truncar si muy largo
        if len(action) > 50:
            action = action[:47] + "..."
        label = f"{i+1}. {col}: {action}"
        lines.append(f'    {node_id}["{label}"]')

    # Conectar secuencialmente
    for i in range(len(top) - 1):
        lines.append(f'    R{i+1} --> R{i+2}')

    # Estilos
    for i, rec in enumerate(top):
        impact = rec.get("estimated_impact", "MEDIUM")
        color = severity_colors.get(impact, "#95a5a6")
        lines.append(f'    style R{i+1} fill:{color},color:#fff')

    lines.append('```')
    return "\n".join(lines)


def _severity_summary_table(issues_by_severity: dict) -> str:
    """Tabla de resumen de severidades con badges."""
    lines = [
        "| Severidad | Cantidad | Indicador |",
        "|-----------|:--------:|-----------|",
    ]
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
        count = issues_by_severity.get(sev, 0)
        badge = SEVERITY_BADGE.get(sev, "")
        bar = "█" * min(count, 20) if count > 0 else "—"
        lines.append(f"| {badge} **{sev}** | {count} | {bar} |")
    return "\n".join(lines)


def _column_issues_table(issues: list) -> str:
    """Tabla compacta de issues de una columna."""
    if not issues:
        return "> ✅ Sin issues detectados\n"
    lines = [
        "| # | Check | Severidad | Detalle |",
        "|:-:|-------|:---------:|---------|",
    ]
    for i, issue in enumerate(issues, 1):
        sev = issue.get("severity", "INFO")
        badge = SEVERITY_BADGE.get(sev, "")
        check = issue.get("check_id", "?")
        msg = issue.get("message", "")
        if len(msg) > 80:
            msg = msg[:77] + "..."
        lines.append(f"| {i} | `{check}` | {badge} {sev} | {msg} |")
    return "\n".join(lines)


def _numeric_stats_table(col_name: str, stats: dict, profiling: dict) -> str:
    """Tabla de estadísticas numéricas compacta."""
    s = stats.get(col_name, {})
    p = profiling.get(col_name, {})
    if not s:
        return ""

    cv = p.get("cv")
    cv_str = f"{cv:.4f}" if cv is not None else "N/A"

    lines = [
        "| Estadística | Valor | | Estadística | Valor |",
        "|-------------|------:|-|-------------|------:|",
        f"| Media | {s.get('mean', 'N/A')} | | Mediana | {s.get('median', 'N/A')} |",
        f"| Std | {s.get('std', 'N/A')} | | CV | {cv_str} |",
        f"| Min | {s.get('min', 'N/A')} | | Max | {s.get('max', 'N/A')} |",
        f"| Skewness | {s.get('skewness', 'N/A')} | | Kurtosis | {s.get('kurtosis', 'N/A')} |",
        f"| Outliers IQR | {s.get('outlier_count_iqr', 0)} | | Outliers Z-score | {s.get('outlier_count_zscore', 0)} |",
    ]
    return "\n".join(lines)


def _categorical_stats_table(col_name: str, stats: dict) -> str:
    """Tabla de estadísticas categóricas."""
    s = stats.get(col_name, {})
    if not s:
        return ""
    rare = ", ".join(s.get("rare_categories", [])[:5]) or "Ninguna"
    lines = [
        "| Estadística | Valor |",
        "|-------------|-------|",
        f"| Valores únicos | {s.get('n_unique', 'N/A')} |",
        f"| Valor más frecuente | `{s.get('top_value', 'N/A')}` |",
        f"| Frecuencia top | {s.get('top_freq', 'N/A'):.1%} |" if isinstance(s.get('top_freq'), (int, float)) else f"| Frecuencia top | {s.get('top_freq', 'N/A')} |",
        f"| Categorías raras | {rare} |",
    ]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# State schema
# ═══════════════════════════════════════════════════════════════════════════

class QualityReportState(TypedDict, total=False):
    report_path: str
    report_data: dict
    overview_md: str
    columns_md: str
    issues_md: str
    final_report: str


# ═══════════════════════════════════════════════════════════════════════════
# LLM setup
# ═══════════════════════════════════════════════════════════════════════════

def _get_llm() -> ChatOpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print(
            "Error: OPENAI_API_KEY no encontrada. "
            "Configúrala en .env o como variable de entorno.",
            file=sys.stderr,
        )
        sys.exit(1)
    return ChatOpenAI(model="gpt-4o-mini", temperature=0.3, api_key=api_key)


# ═══════════════════════════════════════════════════════════════════════════
# Node 1: load_report (Python puro)
# ═══════════════════════════════════════════════════════════════════════════

def load_report(state: QualityReportState) -> dict:
    """Lee y valida el JSON del reporte."""
    report_path = state["report_path"]
    path = Path(report_path)

    if not path.exists():
        print(f"Error: Archivo no encontrado: {report_path}", file=sys.stderr)
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    required_keys = {"report_metadata", "dataset_summary", "column_profiles"}
    missing = required_keys - set(data.keys())
    if missing:
        print(f"Error: Faltan claves en el JSON: {missing}", file=sys.stderr)
        sys.exit(1)

    return {"report_data": data}


# ═══════════════════════════════════════════════════════════════════════════
# Node 2: analyze_overview (Híbrido: visual Python + narrativa LLM)
# ═══════════════════════════════════════════════════════════════════════════

OVERVIEW_PROMPT = """\
Eres un analista de calidad de datos senior. Se te proporcionan los datos de un análisis
de calidad ya ejecutado. Escribe EN ESPAÑOL un párrafo de interpretación narrativa (4-6
oraciones) sobre el estado general de la calidad de estos datos.

Incluye:
- Evaluación general del health score y lo que implica para el uso de los datos
- Mención de las severidades más preocupantes (si las hay)
- Contexto sobre las columnas más y menos saludables
- Una recomendación general de alto nivel

NO generes tablas ni diagramas (ya están generados). Solo escribe el párrafo narrativo.
Sé específico con los números del reporte, no seas genérico."""


def analyze_overview(state: QualityReportState) -> dict:
    """Genera la sección de resumen: visuals determinísticos + narrativa LLM."""
    data = state["report_data"]
    meta = data["report_metadata"]
    summary = data["dataset_summary"]
    sev = summary.get("issues_by_severity", {})
    grade = summary.get("health_grade", "?")
    score = summary.get("health_score", 0)
    light = GRADE_LIGHT.get(grade, "⚪")

    # ── Header ──
    parts = [
        f"# 📊 Reporte de Calidad de Datos — `{meta.get('file_analyzed', '?')}`\n",
        f"> **Generado:** {meta.get('generated_at', '?')[:16]} | "
        f"**Filas:** {meta.get('total_rows', 0):,} | "
        f"**Columnas:** {meta.get('total_columns', 0)} | "
        f"**Encoding:** {meta.get('encoding', '?')}\n",
        "---\n",
        "## 🎯 Resumen Ejecutivo\n",
        f"### Estado General: {light} {score}/100 ({grade})\n",
        _health_bar(score) + "\n",
    ]

    # ── Tabla de severidades ──
    parts.append(_severity_summary_table(sev))
    parts.append("")

    # ── Pie chart ──
    pie = _severity_pie_mermaid(sev)
    if pie:
        parts.append(pie)
        parts.append("")

    # ── Mapa de columnas ──
    col_profiles = data.get("column_profiles", {})
    parts.append("### Mapa de Salud por Columna\n")
    parts.append(_column_health_map_mermaid(col_profiles))
    parts.append("")

    # ── Columnas limpias vs críticas ──
    clean = summary.get("clean_columns", [])
    critical = summary.get("critical_columns", [])
    if clean:
        parts.append(f"**Columnas limpias ({len(clean)}):** {', '.join(f'`{c}`' for c in clean)}\n")
    if critical:
        parts.append(f"**Columnas críticas ({len(critical)}):** {', '.join(f'`{c}`' for c in critical)}\n")

    # ── Narrativa LLM ──
    parts.append("### Interpretación\n")
    llm = _get_llm()
    context_for_llm = {
        "file": meta.get("file_analyzed"),
        "rows": meta.get("total_rows"),
        "cols": meta.get("total_columns"),
        "score": score,
        "grade": grade,
        "issues_by_severity": sev,
        "total_issues": summary.get("total_issues", 0),
        "clean_columns": clean,
        "critical_columns": critical,
        "trend": data.get("quality_trend"),
    }
    response = llm.invoke([
        SystemMessage(content=OVERVIEW_PROMPT),
        HumanMessage(content=json.dumps(context_for_llm, default=str)),
    ])
    parts.append(response.content)
    parts.append("")

    return {"overview_md": "\n".join(parts)}


# ═══════════════════════════════════════════════════════════════════════════
# Node 3: analyze_columns (Híbrido: visual Python + narrativa LLM)
# ═══════════════════════════════════════════════════════════════════════════

COLUMN_INTERPRET_PROMPT = """\
Eres un analista de calidad de datos senior. Para CADA columna en los datos proporcionados,
escribe EN ESPAÑOL una interpretación breve (2-3 oraciones) que explique:
- Qué significan los issues encontrados (si hay)
- Si la distribución parece normal o presenta anomalías
- Si hay algo preocupante que un usuario de datos debería saber
- Para columnas sin issues, confirma que están en buen estado

Responde con un JSON object donde cada key es el nombre de la columna y el value es el
párrafo de interpretación. Ejemplo: {"col1": "La columna...", "col2": "Esta columna..."}

Sé específico con los números. NO uses markdown en los valores, solo texto plano."""


def analyze_columns(state: QualityReportState) -> dict:
    """Genera sección de análisis por columna: cards visuales + interpretación LLM."""
    data = state["report_data"]
    profiles = data.get("column_profiles", {})
    stats_num = data.get("statistical_summary", {}).get("numeric_columns", {})
    stats_cat = data.get("statistical_summary", {}).get("categorical_columns", {})
    stats_date = data.get("statistical_summary", {}).get("date_columns", {})
    profiling = data.get("column_profiling", {})

    # ── Obtener interpretaciones del LLM en un solo call ──
    llm = _get_llm()
    context_for_llm = {
        "column_profiles": profiles,
        "statistical_summary": data.get("statistical_summary", {}),
    }
    response = llm.invoke([
        SystemMessage(content=COLUMN_INTERPRET_PROMPT),
        HumanMessage(content=json.dumps(context_for_llm, default=str)),
    ])

    # Parsear interpretaciones
    interpretations = {}
    try:
        # Extraer JSON del response (puede venir envuelto en markdown code block)
        content = response.content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3]
        interpretations = json.loads(content)
    except (json.JSONDecodeError, IndexError):
        # Fallback: usar el texto completo como interpretación genérica
        interpretations = {}

    # ── Generar markdown para cada columna ──
    parts = [
        "---\n",
        "## 📋 Análisis Detallado por Columna\n",
    ]

    # Ordenar por health_score (peor primero)
    sorted_cols = sorted(profiles.items(), key=lambda x: x[1].get("health_score", 100))

    for col_name, prof in sorted_cols:
        sem_type = prof.get("semantic_type", "?")
        type_icon = TYPE_BADGE.get(sem_type, "❓")
        score = prof.get("health_score", 100)
        grade = prof.get("health_grade", "?")
        light = GRADE_LIGHT.get(grade, "⚪")
        null_pct = prof.get("null_pct", 0)
        n_unique = prof.get("n_unique", 0)
        checks_run = prof.get("checks_run", 0)
        checks_failed = prof.get("checks_failed", 0)
        checks_passed = checks_run - checks_failed
        issues = prof.get("issues", [])

        parts.append(f"### {col_name} {type_icon} `{sem_type}`\n")
        parts.append(
            f"{light} **Health:** {_health_bar(score)} ({grade}) | "
            f"**Nulls:** {null_pct:.1%} | "
            f"**Únicos:** {n_unique:,} | "
            f"**Checks:** {checks_passed}/{checks_run} pasaron\n"
        )

        # Estadísticas según tipo
        if sem_type in ("NUMERIC_CONTINUOUS", "NUMERIC_DISCRETE"):
            table = _numeric_stats_table(col_name, stats_num, profiling)
            if table:
                parts.append(table)
                parts.append("")

            # Histograma
            hist_data = profiling.get(col_name, {}).get("histogram", [])
            if hist_data:
                parts.append("**Distribución:**\n")
                parts.append(_histogram_text(hist_data))
                parts.append("")

        elif sem_type in ("CATEGORICAL", "BOOLEAN"):
            table = _categorical_stats_table(col_name, stats_cat)
            if table:
                parts.append(table)
                parts.append("")

            # Top values como mini bar chart
            top_vals = profiling.get(col_name, {}).get("top_values", [])
            if top_vals:
                max_v = max((v[1] for v in top_vals), default=1) or 1
                parts.append("**Top valores:**\n")
                parts.append("```")
                for val, cnt in top_vals[:7]:
                    bar_len = round(cnt / max_v * 15)
                    parts.append(f"  {str(val):>15s}  {'█' * bar_len} {cnt}")
                parts.append("```\n")

        elif sem_type in ("DATE", "DATETIME"):
            ds = stats_date.get(col_name, {})
            if ds:
                parts.append(
                    f"| Min | Max | Gaps |\n"
                    f"|-----|-----|------|\n"
                    f"| {ds.get('min_date', '?')} | {ds.get('max_date', '?')} | {ds.get('gap_count', 0)} |\n"
                )

        # Issues
        if issues:
            parts.append("**Issues detectados:**\n")
            parts.append(_column_issues_table(issues))
            parts.append("")
        else:
            parts.append("> ✅ **Sin issues detectados** — columna en excelente estado\n")

        # Interpretación LLM
        interp = interpretations.get(col_name, "")
        if interp:
            parts.append(f"💡 *{interp}*\n")

        parts.append("")  # Separador

    return {"columns_md": "\n".join(parts)}


# ═══════════════════════════════════════════════════════════════════════════
# Node 4: analyze_issues (Híbrido: visual Python + narrativa LLM)
# ═══════════════════════════════════════════════════════════════════════════

ISSUES_INTERPRET_PROMPT = """\
Eres un analista de calidad de datos senior. Se te proporcionan los issues críticos y de
alta severidad de un análisis de calidad. Para CADA issue, escribe EN ESPAÑOL:
1. Una oración explicando la causa raíz probable
2. Una oración con el impacto potencial si no se corrige
3. Una oración con la acción correctiva recomendada

Responde con un JSON object donde cada key es "check_id__column" (ej: "HIGH_CORRELATION____dataset__")
y el value es un object {"causa": "...", "impacto": "...", "accion": "..."}.

Sé específico con los datos proporcionados."""


def analyze_issues(state: QualityReportState) -> dict:
    """Genera sección de issues y recomendaciones: visuals + narrativa."""
    data = state["report_data"]
    critical_issues = data.get("critical_issues", [])
    recommendations = data.get("recommendations", [])

    parts = [
        "---\n",
        "## ⚠️ Issues y Recomendaciones\n",
    ]

    # ── LLM interpretaciones ──
    interpretations = {}
    if critical_issues:
        llm = _get_llm()
        response = llm.invoke([
            SystemMessage(content=ISSUES_INTERPRET_PROMPT),
            HumanMessage(content=json.dumps(critical_issues, default=str)),
        ])
        try:
            content = response.content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1] if "\n" in content else content[3:]
                if content.endswith("```"):
                    content = content[:-3]
            interpretations = json.loads(content)
        except (json.JSONDecodeError, IndexError):
            interpretations = {}

    # ── Issues críticos y altos ──
    if critical_issues:
        parts.append("### 🚨 Issues de Alta Severidad\n")
        for issue in critical_issues:
            sev = issue.get("severity", "?")
            badge = SEVERITY_BADGE.get(sev, "")
            col = issue.get("column", "?")
            check = issue.get("check_id", "?")
            msg = issue.get("message", "")
            affected = issue.get("affected_count", 0)
            pct = issue.get("affected_pct", 0)
            samples = issue.get("sample_values", [])

            parts.append(f"#### {badge} {sev}: `{check}` — {col}\n")
            parts.append(f"**Detalle:** {msg}\n")

            if affected > 0:
                parts.append(f"**Registros afectados:** {affected:,} ({pct:.1%})\n")

            if samples:
                parts.append(f"**Muestras:** `{'`, `'.join(str(s) for s in samples[:5])}`\n")

            # Interpretación LLM
            key = f"{check}__{col}"
            interp = interpretations.get(key, {})
            if isinstance(interp, dict):
                if interp.get("causa"):
                    parts.append(f"🔍 **Causa probable:** {interp['causa']}\n")
                if interp.get("impacto"):
                    parts.append(f"⚡ **Impacto:** {interp['impacto']}\n")
                if interp.get("accion"):
                    parts.append(f"🛠️ **Acción:** {interp['accion']}\n")

            parts.append("")

        # Diagrama de correlaciones si aplica
        corr_diagram = _correlation_mermaid(critical_issues)
        if corr_diagram:
            parts.append("### 🔗 Mapa de Correlaciones\n")
            parts.append(corr_diagram)
            parts.append("")
    else:
        parts.append("> ✅ **No se encontraron issues de severidad CRITICAL o HIGH.**\n")
        parts.append("")

    # ── Plan de acción ──
    if recommendations:
        parts.append("### 📋 Plan de Acción Priorizado\n")

        parts.append("| # | Prioridad | Categoría | Columna | Acción |")
        parts.append("|:-:|:---------:|-----------|---------|--------|")
        for rec in recommendations:
            pri = rec.get("priority", "?")
            impact = rec.get("estimated_impact", "?")
            badge = SEVERITY_BADGE.get(impact, "")
            cat = rec.get("category", "?")
            col = rec.get("column", "?")
            action = rec.get("action", "?")
            parts.append(f"| {pri} | {badge} {impact} | {cat} | `{col}` | {action} |")
        parts.append("")

        # Diagrama de flujo de remediación
        parts.append("### 🗺️ Secuencia de Remediación Recomendada\n")
        parts.append(_remediation_flowchart(recommendations))
        parts.append("")

    return {"issues_md": "\n".join(parts)}


# ═══════════════════════════════════════════════════════════════════════════
# Node 5: assemble_report (Híbrido: ensambla + conclusión LLM)
# ═══════════════════════════════════════════════════════════════════════════

CONCLUSION_PROMPT = """\
Eres un analista de calidad de datos senior. Con base en los datos del reporte proporcionado,
escribe EN ESPAÑOL una conclusión final (1 párrafo de 4-5 oraciones) que incluya:
1. Evaluación general de la calidad de los datos
2. Si los datos son aptos para uso en producción/análisis
3. Los 2-3 próximos pasos más importantes
4. Nivel de confianza en los datos (alto/medio/bajo)

Solo texto, sin markdown ni bullets. Sé específico con los datos."""


def assemble_report(state: QualityReportState) -> dict:
    """Ensambla el reporte final: overview + columns + issues + conclusión LLM."""
    data = state["report_data"]
    meta = data["report_metadata"]
    summary = data["dataset_summary"]

    # Conclusión LLM
    llm = _get_llm()
    context = {
        "file": meta.get("file_analyzed"),
        "score": summary.get("health_score"),
        "grade": summary.get("health_grade"),
        "total_issues": summary.get("total_issues"),
        "issues_by_severity": summary.get("issues_by_severity"),
        "critical_columns": summary.get("critical_columns"),
        "clean_columns": summary.get("clean_columns"),
        "n_columns": meta.get("total_columns"),
        "n_rows": meta.get("total_rows"),
    }
    response = llm.invoke([
        SystemMessage(content=CONCLUSION_PROMPT),
        HumanMessage(content=json.dumps(context, default=str)),
    ])

    # Ensamblar
    parts = [
        state["overview_md"],
        state["columns_md"],
        state["issues_md"],
        "---\n",
        "## 📈 Conclusión\n",
        response.content,
        "\n",
        "---\n",
        "*Generado por Data Quality Report Agent (LangGraph + GPT-4o-mini)*\n",
    ]

    return {"final_report": "\n".join(parts)}


# ═══════════════════════════════════════════════════════════════════════════
# Construcción del grafo LangGraph
# ═══════════════════════════════════════════════════════════════════════════

def build_graph():
    """Construye el pipeline StateGraph de LangGraph."""
    graph = StateGraph(QualityReportState)

    graph.add_node("load_report", load_report)
    graph.add_node("analyze_overview", analyze_overview)
    graph.add_node("analyze_columns", analyze_columns)
    graph.add_node("analyze_issues", analyze_issues)
    graph.add_node("assemble_report", assemble_report)

    graph.add_edge(START, "load_report")
    # Fan-out: 3 análisis en paralelo
    graph.add_edge("load_report", "analyze_overview")
    graph.add_edge("load_report", "analyze_columns")
    graph.add_edge("load_report", "analyze_issues")
    # Fan-in: convergen en ensamblaje
    graph.add_edge("analyze_overview", "assemble_report")
    graph.add_edge("analyze_columns", "assemble_report")
    graph.add_edge("analyze_issues", "assemble_report")
    graph.add_edge("assemble_report", END)

    return graph.compile()


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Genera un reporte visual de calidad de datos potenciado por IA."
    )
    parser.add_argument(
        "--input", required=True,
        help="Ruta al report.json generado por data_quality_auditor.py",
    )
    parser.add_argument(
        "--output", default=None,
        help="Ruta de salida para el reporte Markdown (default: reporte_calidad.md junto al input)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.parent / "reporte_calidad.md"

    print(f"  📂 Cargando reporte: {input_path}")

    app = build_graph()
    result = app.invoke({"report_path": str(input_path)})

    final_md = result["final_report"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_md)

    print(f"  ✅ Reporte generado: {output_path}")
    print(f"  📏 Tamaño: {len(final_md):,} caracteres")


if __name__ == "__main__":
    main()
