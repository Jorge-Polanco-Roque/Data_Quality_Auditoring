# Data Quality Auditor

Framework de auditoría automática de calidad de datos para archivos CSV. Analiza cualquier CSV sin configuración previa, detecta problemas estadísticos y genera reportes accionables.

## Características

- **70+ checks automáticos**: nulls, outliers, duplicados, formato, distribución, tendencia, hipótesis, Benford, PII, correlación, multicolinealidad, estacionariedad
- **13 tipos semánticos**: detección automática (numérico continuo/discreto, fecha, categórico, booleano, email, teléfono, ID, texto, etc.)
- **7 formatos de reporte**: JSON, Markdown, HTML interactivo (Chart.js), texto plano, Excel con formato, resumen ejecutivo, CSV de filas flaggeadas
- **Pruebas de hipótesis adaptativas**: gate de normalidad (Shapiro/D'Agostino/Anderson-Darling/Lilliefors) → paramétrico o no paramétrico
- **Reglas de negocio**: DSL en YAML para validaciones condicionales inter-columna
- **Detección de PII**: emails, teléfonos, tarjetas de crédito, SSN, CURP, RFC, IPs
- **Scoring configurable**: pesos por severidad y por columna ajustables via YAML
- **Trend histórico**: compara scores entre corridas del mismo CSV
- **Modo batch**: procesa directorios completos de CSVs con reporte consolidado
- **Drift detection**: compara dos datasets (KS test, chi², schema diff)
- **Integridad referencial**: valida foreign keys entre CSVs
- **Schema validation**: tipos, rangos, patrones, llaves compuestas, valores permitidos

## Instalación

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Uso rápido

```bash
# Auditoría básica — genera todos los reportes en outputs/001_nombre/
python data_quality_auditor.py --input datos.csv

# Con schema y config personalizados
python data_quality_auditor.py --input datos.csv --schema schema.yaml --config config.yaml

# Modo batch
python data_quality_auditor.py --batch ./carpeta_csvs/

# Drift detection
python data_quality_auditor.py --input actual.csv --compare referencia.csv

# Modo silencioso (solo exit code)
python data_quality_auditor.py --input datos.csv --quiet
```

## Outputs

Cada corrida genera automáticamente una carpeta numerada en `outputs/`:

```
outputs/001_datos/
├── report.json              # Reporte estructurado completo
├── report.md                # Markdown con tablas y emojis
├── report.html              # HTML interactivo con gráficos Chart.js
├── report.txt               # Texto plano
├── report.xlsx              # Excel con pestañas formateadas
├── executive_summary.md     # Resumen ejecutivo de 1 página
└── flagged_rows.csv         # Filas problemáticas con motivo exacto
```

## Exit Codes

| Código | Significado |
|--------|-------------|
| 0 | Sin issues |
| 1 | Issues encontrados (no críticos) |
| 2 | Issues críticos detectados |

## Scoring

Cada columna inicia con score 100 y se deducen puntos por issue:

| Severidad | Deducción default |
|-----------|------------------|
| CRITICAL | -25 |
| HIGH | -10 |
| MEDIUM | -5 |
| LOW | -2 |
| INFO | 0 |

Score del dataset = promedio ponderado de columnas. Grados: A (≥90), B (≥75), C (≥60), D (≥40), F (<40).

Todos los pesos son configurables via `config.yaml`.

## Config YAML

```yaml
disabled_checks:
  - BENFORD_LAW
  - ADF_STATIONARITY

severity_overrides:
  MEAN_SHIFT: LOW

scoring:
  CRITICAL: 30
  HIGH: 15

column_weights:
  monto: 5.0
  id_cliente: 3.0

business_rules:
  - name: "Reembolso requiere cancelación"
    condition: "status == 'cancelado'"
    assertion: "monto_reembolso > 0"
    severity: HIGH

foreign_keys:
  - child_table: ordenes.csv
    child_column: id_cliente
    parent_table: clientes.csv
    parent_column: id
```

## Schema YAML

```yaml
columns:
  nombre:
    type: categorical
    not_null: true
  monto:
    type: numeric
    min: 0
    max: 100000
  email:
    type: email
    pattern: "^[a-z]+@empresa\\.com$"

composite_keys:
  - [id_orden, id_producto]
```

## Tests

```bash
python -m pytest tests/ -v
```

89 tests unitarios cubriendo: checks universales, numéricos, categóricos, hipótesis, Benford, cross-column, null patterns, schema, pipeline, PII, business rules, flagged rows, executive summary, Excel, scoring configurable, integridad referencial, trend analyzer.

## Arquitectura

```
CSV → DataLoader → TypeDetector → CheckRegistry → CheckEngine → ScoringSystem → ReportBuilder
         │              │               │               │              │              │
     encoding       13 tipos       checks por       per-column     100-point      JSON/MD/HTML/
     delimiter      semánticos     tipo semántico   + dataset      configurable   XLSX/TXT/Exec
     sampling                                       + PII                         + flagged_rows
                                                    + business
                                                      rules
```

## Tecnologías

pandas, numpy, scipy, statsmodels, chardet, rapidfuzz, python-dateutil, pymannkendall, openpyxl, pyyaml.
