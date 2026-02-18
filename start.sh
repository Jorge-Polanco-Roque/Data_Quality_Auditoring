#!/usr/bin/env bash
# ============================================================================
#  start.sh — Análisis de Calidad de Datos (one-click)
#
#  Uso:
#    1. Coloca tu archivo CSV en la carpeta pon_aqui_el_reporte_a_analizar/
#    2. Ejecuta:  ./start.sh
#    3. El reporte visual se genera en la carpeta resultado/
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="$SCRIPT_DIR/pon_aqui_el_reporte_a_analizar"
OUTPUT_DIR="$SCRIPT_DIR/resultado"

# ── Colores para output ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${CYAN}║       📊  Data Quality Auditor — Análisis Visual        ║${NC}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# ── Verificar que existe la carpeta de entrada ──
if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${RED}Error: No existe la carpeta '$INPUT_DIR'${NC}"
    echo "Créala y coloca tu archivo CSV ahí."
    exit 1
fi

# ── Buscar CSV en la carpeta ──
CSV_FILES=( "$INPUT_DIR"/*.csv )

if [ ! -f "${CSV_FILES[0]}" ]; then
    echo -e "${RED}Error: No se encontró ningún archivo .csv en:${NC}"
    echo "  $INPUT_DIR"
    echo ""
    echo -e "${YELLOW}Coloca tu archivo CSV ahí y vuelve a ejecutar ./start.sh${NC}"
    exit 1
fi

# Tomar el primer CSV encontrado
CSV_FILE="${CSV_FILES[0]}"
CSV_NAME="$(basename "$CSV_FILE")"

if [ ${#CSV_FILES[@]} -gt 1 ]; then
    echo -e "${YELLOW}Se encontraron múltiples CSVs. Usando el primero: ${BOLD}$CSV_NAME${NC}"
else
    echo -e "${GREEN}CSV encontrado: ${BOLD}$CSV_NAME${NC}"
fi

# ── Activar venv ──
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
elif [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
else
    echo -e "${YELLOW}Nota: No se encontró entorno virtual. Usando Python del sistema.${NC}"
fi

# ── Verificar dependencias ──
python -c "import pandas, langgraph, langchain_openai, dotenv" 2>/dev/null || {
    echo -e "${YELLOW}Instalando dependencias...${NC}"
    pip install -r "$SCRIPT_DIR/requirements.txt" -q
}

# ── Verificar OPENAI_API_KEY ──
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

if [ -z "${OPENAI_API_KEY:-}" ]; then
    echo ""
    echo -e "${RED}Error: OPENAI_API_KEY no configurada.${NC}"
    echo ""
    echo "Opciones:"
    echo "  1. Crea un archivo .env con:  OPENAI_API_KEY=sk-..."
    echo "  2. O expórtala:  export OPENAI_API_KEY=sk-..."
    echo ""
    exit 1
fi

# ── Preparar carpeta de resultado ──
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# ── Paso 1: Ejecutar auditoría ──
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  Paso 1/2: Ejecutando auditoría de calidad...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Run auditor with output in resultado/, capture exit code (1 = issues found is OK)
set +e
python "$SCRIPT_DIR/data_quality_auditor.py" \
    --input "$CSV_FILE" \
    --no-auto-output \
    --output "$OUTPUT_DIR/report.json" \
    --quiet
AUDIT_EXIT=$?
set -e

if [ $AUDIT_EXIT -eq 2 ]; then
    echo -e "${RED}  ⚠  Se encontraron issues CRÍTICOS en los datos${NC}"
elif [ $AUDIT_EXIT -eq 1 ]; then
    echo -e "${YELLOW}  ⚡ Se encontraron issues en los datos${NC}"
else
    echo -e "${GREEN}  ✔  Datos sin issues${NC}"
fi

# Verificar que se generó el JSON
if [ ! -f "$OUTPUT_DIR/report.json" ]; then
    echo -e "${RED}Error: No se generó report.json${NC}"
    exit 1
fi

echo -e "${GREEN}  ✔  Auditoría completada → resultado/report.json${NC}"

# ── Paso 2: Generar reporte visual con LLM ──
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  Paso 2/2: Generando reporte visual con IA...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

python "$SCRIPT_DIR/quality_report_agent.py" \
    --input "$OUTPUT_DIR/report.json" \
    --output "$OUTPUT_DIR/reporte_calidad.md"

echo ""
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}║                   ✅  ¡Listo!                           ║${NC}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  📁 Resultados en: ${BOLD}resultado/${NC}"
echo -e "     📄 report.json          — Datos crudos del análisis"
echo -e "     📊 reporte_calidad.md   — Reporte visual (abrir con visor Markdown)"
echo ""
