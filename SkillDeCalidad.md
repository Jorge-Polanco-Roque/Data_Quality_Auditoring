---
name: data-quality-auditor
description: >
  Framework experto para construir un auditor de calidad de datos dinámico en Python.
  Recibe cualquier CSV, detecta automáticamente tipos de dato, ejecuta pruebas
  estadísticas y heurísticas por tipo, y genera un reporte estandarizado con
  severidad de issues. Usar cuando se necesite: (1) Construir un script de calidad
  de datos reutilizable, (2) Detectar anomalías, outliers, nulls y errores de formato,
  (3) Identificar cambios de tendencia y drift estadístico, (4) Generar reportes
  ejecutivos con puntos críticos clasificados por severidad.
---

# Data Quality Auditor — Skill de Construcción

Framework experto para construir `data_quality_auditor.py`, un sistema dinámico de
auditoría de calidad que funciona sobre cualquier CSV sin configuración previa.

---

## Arquitectura del Sistema

El script se divide en **6 capas independientes** que se ejecutan en cadena:

```
CSV Input
   │
   ▼
[1] DataLoader          ← Carga, encoding, delimiters
   │
   ▼
[2] TypeDetector        ← Infiere tipo semántico de cada columna
   │
   ▼
[3] CheckRegistry       ← Mapa de checks por tipo de dato
   │
   ▼
[4] CheckEngine         ← Ejecuta checks, captura resultados
   │
   ▼
[5] ScoringSystem       ← Asigna severidad (CRITICAL/HIGH/MEDIUM/LOW/INFO)
   │
   ▼
[6] ReportBuilder       ← Genera reporte estandarizado JSON + texto
```

---

## Capa 1 — DataLoader

**Responsabilidad:** Cargar el CSV de forma robusta sin asumir nada del archivo.

**Lógica de implementación:**
- Detectar encoding con `chardet` antes de leer con pandas
- Intentar delimiters en orden: `,` → `;` → `\t` → `|`; usar el que produzca más columnas
- Leer todo como `dtype=str` primero (preservar datos crudos para inspección de formato)
- Generar `df_raw` (strings puros) y `df` (con tipos inferidos por pandas) en paralelo
- Registrar metadata: `n_rows`, `n_cols`, `file_size_mb`, `encoding`, `delimiter`

**Output:** `(df_raw, df, metadata_dict)`

---

## Capa 2 — TypeDetector

**Responsabilidad:** Asignar un **tipo semántico** a cada columna, más allá del dtype de pandas.

### Tipos Semánticos Soportados

| Tipo Semántico     | Descripción                                              |
|--------------------|----------------------------------------------------------|
| `NUMERIC_CONTINUOUS` | Floats o ints con alta cardinalidad (precios, métricas) |
| `NUMERIC_DISCRETE`   | Ints con baja cardinalidad (conteos, ratings, edades)   |
| `CATEGORICAL`        | Strings con cardinalidad baja relativa a n_rows         |
| `HIGH_CARDINALITY`   | Strings con cardinalidad >50% de n_rows                 |
| `BOOLEAN`            | Columnas con 2 valores únicos (true/false, 0/1, si/no)  |
| `DATE`               | Columnas parseables como fecha                          |
| `DATETIME`           | Columnas parseables como fecha+hora                     |
| `EMAIL`              | Strings que coinciden con patrón de email               |
| `PHONE`              | Strings que coinciden con patrones telefónicos          |
| `ID_CANDIDATE`       | Alta unicidad + patrón estructurado (UUID, código)      |
| `MIXED`              | Columna con mezcla de tipos detectados                  |
| `EMPTY`              | >95% valores nulos o vacíos                             |
| `CONSTANT`           | Un solo valor único en toda la columna                  |

### Algoritmo de Detección

Para cada columna, ejecutar en este orden (el primero que aplique gana):

```
1. Si null_pct >= 0.95  → EMPTY
2. Si n_unique == 1     → CONSTANT
3. Si n_unique == 2 y valores son variantes de true/false/0/1/si/no → BOOLEAN
4. Si pandas dtype es numeric:
     Si n_unique / n_rows_nonnull < 0.05 → NUMERIC_DISCRETE
     Else → NUMERIC_CONTINUOUS
5. Si pandas dtype es object:
     a. Intentar parsear muestra de 200 valores como fecha → si >80% parsea → DATE o DATETIME
     b. Regex email sobre muestra → si >80% → EMAIL
     c. Regex phone sobre muestra → si >80% → PHONE
     d. Si n_unique / n_rows_nonnull > 0.85 → ID_CANDIDATE o HIGH_CARDINALITY
     e. Si n_unique / n_rows_nonnull < 0.15 → CATEGORICAL
     f. Else → HIGH_CARDINALITY
6. Si hay mezcla detectada en pasos anteriores → MIXED
```

**Para fechas:** intentar múltiples formatos: ISO 8601, `dd/mm/yyyy`, `mm/dd/yyyy`,
`yyyy-mm-dd HH:MM:SS`, `dd-Mon-yyyy`, Unix timestamp numérico.
Registrar el formato dominante y los formatos alternativos encontrados.

**Output:** `column_type_map: Dict[str, SemanticType]`

---

## Capa 3 — CheckRegistry

**Responsabilidad:** Mapa declarativo de qué checks aplican a cada tipo semántico.

### Estructura del Registro

Cada check es un objeto con:
- `check_id`: identificador único (`"NULL_RATE"`, `"OUTLIER_IQR"`, etc.)
- `applies_to`: lista de tipos semánticos donde corre
- `function`: callable que recibe `(series_raw, series_typed, metadata)` y retorna `CheckResult`
- `severity_rules`: dict de umbrales → severidad

### Mapa de Checks por Tipo

#### Checks Universales (todos los tipos)
| Check ID              | Descripción                                          |
|-----------------------|------------------------------------------------------|
| `NULL_RATE`           | % de nulos/NaN/strings vacíos                        |
| `DUPLICATE_ROWS`      | Filas completamente duplicadas (solo en nivel global)|
| `WHITESPACE_ISSUES`   | Valores con espacios leading/trailing                |
| `CONSTANT_COLUMN`     | Columna con un solo valor único                      |
| `NEAR_CONSTANT`       | Un valor representa >95% de los registros            |

#### Checks Numéricos (`NUMERIC_CONTINUOUS`, `NUMERIC_DISCRETE`)
| Check ID              | Descripción                                          |
|-----------------------|------------------------------------------------------|
| `OUTLIER_IQR`         | Valores fuera de 1.5×IQR (Tukey)                    |
| `OUTLIER_ZSCORE`      | Valores con \|z-score\| > 3                          |
| `OUTLIER_MODIFIED_Z`  | Modified Z-score con MAD para distribuciones sesgadas|
| `DISTRIBUTION_SKEW`   | Skewness > 2 o < -2 (distribución muy asimétrica)   |
| `DISTRIBUTION_KURTOSIS` | Kurtosis excesiva (colas pesadas)                  |
| `NEGATIVE_VALUES`     | Presencia de negativos en columnas que no deberían   |
| `ZERO_VALUES`         | % de ceros (puede indicar valores faltantes codificados)|
| `TREND_CHANGE`        | Cambio significativo en media móvil vs histórico     |
| `VALUE_RANGE`         | Valores fuera del rango percentil [0.1, 99.9]        |
| `VARIANCE_SUDDEN_CHANGE` | Cambio abrupto en varianza entre segmentos del df |
| `NORMALITY_TEST`      | Shapiro-Wilk (n<5000) o D'Agostino-K² para normalidad|

#### Checks de Fechas (`DATE`, `DATETIME`)
| Check ID              | Descripción                                          |
|-----------------------|------------------------------------------------------|
| `DATE_NULL_RATE`      | Nulos en columna de fecha (crítico para series temporales)|
| `DATE_FORMAT_MIX`     | Múltiples formatos de fecha en la misma columna      |
| `DATE_FUTURE`         | Fechas futuras (si no se esperan)                    |
| `DATE_ANCIENT`        | Fechas antes de 1900 (posible error de dato)         |
| `DATE_SEQUENCE_GAPS`  | Gaps inesperados en series temporales               |
| `DATE_DUPLICATES`     | Fechas duplicadas (si se espera unicidad)            |
| `DATE_MONOTONICITY`   | Verifica que la columna esté ordenada si debería     |
| `DATE_INVALID_PARSED` | Valores que no pudieron parsearse como fecha válida  |
| `TEMPORAL_DRIFT`      | Cambio en distribución de valores a lo largo del tiempo |

#### Checks Categóricos (`CATEGORICAL`, `BOOLEAN`)
| Check ID              | Descripción                                          |
|-----------------------|------------------------------------------------------|
| `RARE_CATEGORIES`     | Categorías con frecuencia < 0.5% del total           |
| `CARDINALITY_CHANGE`  | Nuevas categorías vs las esperadas (si hay referencia)|
| `CASE_INCONSISTENCY`  | Misma categoría con diferente capitalización         |
| `ENCODING_ANOMALY`    | Caracteres raros o de control en categorías          |
| `CLASS_IMBALANCE`     | Una categoría representa >95% de los datos           |
| `TYPO_CANDIDATES`     | Categorías similares por distancia de Levenshtein    |

#### Checks de Texto (`HIGH_CARDINALITY`, `EMAIL`, `PHONE`)
| Check ID              | Descripción                                          |
|-----------------------|------------------------------------------------------|
| `EMAIL_FORMAT`        | Emails que no cumplen RFC 5322 básico                |
| `PHONE_FORMAT`        | Teléfonos que no cumplen patrón esperado (E.164 o local)|
| `LENGTH_OUTLIERS`     | Longitud de string muy fuera del rango típico        |
| `NULL_LIKE_STRINGS`   | Strings que son "N/A", "null", "none", "NA", "NaN", "-"|
| `TRUNCATION_SIGNS`    | Valores que terminan abruptamente (posible truncación)|

#### Checks de IDs (`ID_CANDIDATE`)
| Check ID              | Descripción                                          |
|-----------------------|------------------------------------------------------|
| `ID_DUPLICATES`       | IDs duplicados (generalmente crítico)                |
| `ID_FORMAT_CONSISTENCY` | Patrón de formato inconsistente en IDs             |
| `ID_NULL`             | Nulos en columna de ID                               |

---

## Capa 4 — CheckEngine

**Responsabilidad:** Ejecutar todos los checks aplicables y capturar resultados de forma segura.

### CheckResult Schema

```python
@dataclass
class CheckResult:
    check_id: str
    column: str
    passed: bool
    severity: str        # CRITICAL | HIGH | MEDIUM | LOW | INFO | PASS
    value: float         # valor medido (ej: 0.23 para 23% de nulls)
    threshold: float     # umbral que se violó
    message: str         # descripción legible
    affected_count: int  # n° de registros afectados
    affected_pct: float  # % de registros afectados
    sample_values: list  # hasta 5 ejemplos de valores problemáticos
    metadata: dict       # datos adicionales del check
```

### Implementación de Checks Clave

#### NULL_RATE
```python
null_pct = (series.isna() | (series.astype(str).str.strip() == '')).mean()
thresholds = {0.5: 'CRITICAL', 0.2: 'HIGH', 0.05: 'MEDIUM', 0.01: 'LOW'}
```

#### OUTLIER_IQR
```python
Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
IQR = Q3 - Q1
mask = (series < Q1 - 1.5*IQR) | (series > Q3 + 1.5*IQR)
# Severidad basada en outlier_pct y magnitud de desviación
```

#### OUTLIER_MODIFIED_Z (robusto para distribuciones no normales)
```python
median = series.median()
MAD = (series - median).abs().median()
modified_z = 0.6745 * (series - median) / MAD
mask = modified_z.abs() > 3.5
```

#### TREND_CHANGE (detección de drift)
```python
# Dividir serie en N ventanas temporales iguales
# Calcular media de cada ventana
# Comparar cada ventana vs media global: si |delta| > 2*std_global → alerta
# También: Mann-Kendall test para tendencia monotónica
```

#### DATE_FORMAT_MIX
```python
# Para cada valor no-nulo, intentar parsear con N formatos conocidos
# Registrar qué formato parseó cada valor
# Si hay más de 1 formato activo → HIGH
# Si hay >3 formatos activos → CRITICAL
```

#### TYPO_CANDIDATES (categorías similares)
```python
from rapidfuzz import fuzz
# Para cada par de categorías únicas con frecuencia > 1
# Si Levenshtein similarity > 85% y son diferentes → candidatos a typo
# Reportar pares sospechosos
```

#### TEMPORAL_DRIFT
```python
# Si existe columna de fecha, ordenar df por ella
# Dividir en cuartiles temporales
# Para cada columna numérica: comparar distribución en Q1 vs Q4
# Usar KS test (scipy.stats.ks_2samp): si p < 0.05 → drift significativo
```

### Manejo de Errores en Engine
- Cada check corre en `try/except`; si falla, genera `CheckResult` con `severity='INFO'` y mensaje de error
- No detener el análisis completo por fallo de un check individual
- Loggear warnings internos sin romper el flujo

---

## Capa 5 — ScoringSystem

**Responsabilidad:** Agregar resultados y calcular score de salud por columna y global.

### Severity Levels

| Nivel      | Descripción                                           | Acción Recomendada                    |
|------------|-------------------------------------------------------|---------------------------------------|
| `CRITICAL` | Problema grave que compromete la usabilidad del dato  | Detener pipeline, investigar de inmediato |
| `HIGH`     | Problema significativo que afecta análisis             | Resolver antes de cualquier uso        |
| `MEDIUM`   | Problema moderado, puede sesgar resultados             | Documentar y evaluar impacto           |
| `LOW`      | Anomalía menor, probablemente aceptable                | Registrar y monitorear                 |
| `INFO`     | Observación sin impacto directo                        | Opcional revisión                      |
| `PASS`     | Check superado sin problemas                           | —                                      |

### Column Health Score

```
score = 100
- por cada CRITICAL: -25 puntos
- por cada HIGH:     -10 puntos
- por cada MEDIUM:   -5 puntos
- por cada LOW:      -2 puntos
score = max(0, score)

Grade: A (90-100) | B (75-89) | C (60-74) | D (40-59) | F (<40)
```

### Dataset Health Score
```
dataset_score = media ponderada de column scores
weight por columna = 1 / (1 + null_pct)  ← columnas más completas pesan más
```

---

## Capa 6 — ReportBuilder

**Responsabilidad:** Generar reporte estandarizado completo en múltiples formatos.

### Estructura del Reporte JSON

```json
{
  "report_metadata": {
    "generated_at": "ISO timestamp",
    "file_analyzed": "nombre del archivo",
    "total_rows": 0,
    "total_columns": 0,
    "encoding": "utf-8",
    "delimiter": ","
  },
  "dataset_summary": {
    "health_score": 0.0,
    "health_grade": "A|B|C|D|F",
    "total_issues": 0,
    "issues_by_severity": {
      "CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0
    },
    "critical_columns": ["col1", "col2"],
    "clean_columns": ["col3"]
  },
  "column_profiles": {
    "column_name": {
      "semantic_type": "NUMERIC_CONTINUOUS",
      "pandas_dtype": "float64",
      "n_unique": 0,
      "null_pct": 0.0,
      "health_score": 0.0,
      "health_grade": "A",
      "checks_run": 0,
      "checks_failed": 0,
      "issues": []
    }
  },
  "critical_issues": [
    {
      "check_id": "NULL_RATE",
      "column": "col1",
      "severity": "CRITICAL",
      "message": "53% de valores nulos",
      "affected_count": 530,
      "affected_pct": 0.53,
      "sample_values": []
    }
  ],
  "recommendations": [
    {
      "priority": 1,
      "category": "Missing Data",
      "column": "col1",
      "action": "Investigar fuente de nulls; evaluar imputación o exclusión",
      "estimated_impact": "HIGH"
    }
  ],
  "statistical_summary": {
    "numeric_columns": {
      "col_name": {
        "mean": 0.0, "median": 0.0, "std": 0.0,
        "min": 0.0, "max": 0.0,
        "skewness": 0.0, "kurtosis": 0.0,
        "outlier_count_iqr": 0, "outlier_count_zscore": 0
      }
    },
    "categorical_columns": {
      "col_name": {
        "n_unique": 0, "top_value": "", "top_freq": 0.0,
        "rare_categories": []
      }
    },
    "date_columns": {
      "col_name": {
        "min_date": "", "max_date": "",
        "formats_found": [], "gap_count": 0
      }
    }
  }
}
```

### Reporte de Texto (stdout / .txt)

El reporte de texto debe seguir esta plantilla estandarizada:

```
╔══════════════════════════════════════════════════════════════╗
║           DATA QUALITY AUDIT REPORT                         ║
╚══════════════════════════════════════════════════════════════╝

Archivo     : {filename}
Filas       : {n_rows:,}
Columnas    : {n_cols}
Generado    : {timestamp}
Health Score: {score}/100  ({grade})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESUMEN DE ISSUES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🔴 CRITICAL : {n_critical}
  🟠 HIGH     : {n_high}
  🟡 MEDIUM   : {n_medium}
  🟢 LOW      : {n_low}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PUNTOS CRÍTICOS (requieren acción inmediata)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{por cada issue CRITICAL o HIGH}
  [{severity}] {column} → {check_id}
  Detalle   : {message}
  Afectados : {affected_count:,} registros ({affected_pct:.1%})
  Muestra   : {sample_values}
  ─────────────────────────────────────────────

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REPORTE POR COLUMNA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{por cada columna}
  {col_name} [{semantic_type}] — Score: {score}/100 ({grade})
  Nulls: {null_pct:.1%} | Únicos: {n_unique:,}
  Issues: {lista de issues con severidad}
  ─────────────────────────────────────────────

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RECOMENDACIONES PRIORIZADAS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{ordenadas por prioridad}
  #{n}. [{category}] {column}: {action}
```

---

## Estructura de Archivos del Proyecto

```
data_quality_auditor/
│
├── data_quality_auditor.py        ← Entry point principal (CLI)
│
├── core/
│   ├── __init__.py
│   ├── data_loader.py             ← Capa 1
│   ├── type_detector.py           ← Capa 2
│   ├── check_registry.py          ← Capa 3 (definición declarativa)
│   ├── check_engine.py            ← Capa 4
│   ├── scoring_system.py          ← Capa 5
│   └── report_builder.py          ← Capa 6
│
├── checks/
│   ├── __init__.py
│   ├── universal_checks.py        ← Checks para todos los tipos
│   ├── numeric_checks.py          ← Checks numéricos
│   ├── date_checks.py             ← Checks de fechas
│   ├── categorical_checks.py      ← Checks categóricos
│   ├── text_checks.py             ← Checks de texto/email/phone
│   └── id_checks.py               ← Checks de IDs
│
├── models/
│   ├── __init__.py
│   ├── check_result.py            ← Dataclass CheckResult
│   └── semantic_type.py           ← Enum SemanticType
│
└── requirements.txt
```

---

## Requirements

```txt
pandas>=2.0.0
numpy>=1.24.0
scipy>=1.11.0
chardet>=5.0.0
rapidfuzz>=3.0.0       # Para detección de typos en categóricos
python-dateutil>=2.8.0 # Para parsing flexible de fechas
pymannkendall>=1.4.3   # Para Mann-Kendall trend test
rich>=13.0.0           # Para reporte en consola con colores
```

---

## CLI Interface

```bash
# Uso básico
python data_quality_auditor.py --input data.csv

# Con output de reporte
python data_quality_auditor.py --input data.csv --output report.json

# Solo mostrar issues CRITICAL y HIGH
python data_quality_auditor.py --input data.csv --min-severity HIGH

# Definir columna de fecha para análisis temporal
python data_quality_auditor.py --input data.csv --date-col fecha

# Exportar reporte de texto también
python data_quality_auditor.py --input data.csv --output report.json --text-report report.txt

# Modo silencioso (solo exit code: 0=ok, 1=issues, 2=critical)
python data_quality_auditor.py --input data.csv --quiet
```

---

## Guía de Implementación para Claude

Cuando el usuario pida construir este proyecto, seguir este orden:

**Paso 1 — Modelos base**
Crear `models/semantic_type.py` (Enum) y `models/check_result.py` (dataclass).
Estos no tienen dependencias y todo lo demás los usa.

**Paso 2 — DataLoader**
Implementar `core/data_loader.py` con detección de encoding y delimiter.
Probar que carga correctamente antes de continuar.

**Paso 3 — TypeDetector**
Implementar `core/type_detector.py` siguiendo el algoritmo de detección en el orden
exacto descrito en Capa 2. Incluir todos los tipos semánticos del enum.

**Paso 4 — Checks (empezar por universales y numéricos)**
Implementar checks en `checks/` uno por módulo. Empezar con `universal_checks.py`
y `numeric_checks.py` que son los más usados.

**Paso 5 — CheckRegistry + CheckEngine**
Registrar todos los checks con su mapeo de tipos y ejecutar en cadena.

**Paso 6 — Scoring + ReportBuilder**
Implementar scoring y generar ambos formatos de reporte (JSON + texto).

**Paso 7 — CLI**
Usar `argparse` para el entry point. Retornar exit code según severidad máxima.

### Principios de Implementación

- **Nunca asumir nada del CSV:** todo se detecta o se maneja con fallback
- **Fail-safe:** cada check en try/except, error = INFO result, nunca crash total
- **Muestra de valores:** siempre incluir hasta 5 ejemplos de valores problemáticos
- **Reproducibilidad:** el reporte debe ser determinístico dado el mismo input
- **Performance:** para DFs > 100k filas, usar muestreo estratificado en checks costosos
  (outlier detection, typo detection) con nota en el reporte
- **Sin dependencias de ML:** solo estadística clásica para máxima portabilidad

### Umbrales por Defecto (Configurables)

```python
THRESHOLDS = {
    "null_rate":          {"CRITICAL": 0.50, "HIGH": 0.20, "MEDIUM": 0.05, "LOW": 0.01},
    "outlier_pct_iqr":    {"CRITICAL": 0.10, "HIGH": 0.05, "MEDIUM": 0.02, "LOW": 0.005},
    "outlier_pct_zscore": {"CRITICAL": 0.05, "HIGH": 0.02, "MEDIUM": 0.01},
    "skewness_abs":       {"HIGH": 3.0, "MEDIUM": 2.0, "LOW": 1.0},
    "duplicate_row_pct":  {"CRITICAL": 0.10, "HIGH": 0.05, "MEDIUM": 0.01},
    "rare_category_pct":  {"threshold": 0.005},   # categorías con < 0.5% de aparición
    "id_duplicate_pct":   {"CRITICAL": 0.001},    # cualquier duplicado en ID es HIGH+
    "trend_change_std":   {"CRITICAL": 3.0, "HIGH": 2.5, "MEDIUM": 2.0},  # desviaciones
    "date_format_mix":    {"HIGH": 2, "CRITICAL": 4},  # n° de formatos distintos
    "class_imbalance":    {"HIGH": 0.95, "MEDIUM": 0.90},
    "zero_value_pct":     {"HIGH": 0.30, "MEDIUM": 0.10},  # para columnas numéricas
    "levenshtein_sim":    {"threshold": 0.85},    # umbral para typo candidates
    "ks_pvalue":          {"threshold": 0.05},    # para temporal drift
}
```

---

## Patrones de Detección Especiales

### Detección de Nulls Enmascarados
Tratar como nulos los siguientes patrones en strings:
```python
NULL_LIKE = {
    '', 'null', 'none', 'nan', 'na', 'n/a', 'n.a.', '-', '--', '---',
    'missing', 'unknown', 'undefined', '?', 'nil', '#n/a', 'not available',
    'not applicable', 'sin dato', 'sin información', 'desconocido'
}
# Comparar en lowercase y stripped
```

### Detección de Formatos de Fecha Mixtos
```python
DATE_FORMATS = [
    '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y',
    '%Y/%m/%d', '%d.%m.%Y', '%Y%m%d',
    '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ',
    '%d %b %Y', '%B %d, %Y', '%d de %B de %Y'  # formatos en español
]
```

### Detección de Cambio de Tendencia
Para columnas numéricas, si existe una columna de fecha:
1. Ordenar por fecha
2. Calcular media móvil de ventana = max(7, n_rows // 20)
3. Comparar cada punto con la banda [global_mean ± N*global_std]
4. Si >5% de puntos caen fuera en el último 20% del período → TREND_CHANGE alert
5. Complementar con Mann-Kendall monotonic trend test (p-value + dirección)

---

## Ejemplo de Output Esperado

Para un CSV de ventas con columnas: `fecha`, `producto`, `monto`, `cliente_id`, `región`:

```
╔═══════════════════════════════════════════════════════╗
║           DATA QUALITY AUDIT REPORT                  ║
╚═══════════════════════════════════════════════════════╝
Archivo     : ventas_q3.csv
Filas       : 45,230
Columnas    : 5
Health Score: 61/100  (C)

RESUMEN DE ISSUES
  🔴 CRITICAL : 1
  🟠 HIGH     : 3
  🟡 MEDIUM   : 2
  🟢 LOW      : 1

PUNTOS CRÍTICOS
  [CRITICAL] monto → NULL_RATE
  Detalle   : 52.3% de valores nulos en columna numérica clave
  Afectados : 23,655 registros (52.3%)
  Muestra   : [NaN, NaN, NaN, NaN, NaN]

  [HIGH] fecha → DATE_FORMAT_MIX
  Detalle   : 3 formatos de fecha distintos encontrados
  Afectados : 1,204 registros (2.7%)
  Muestra   : ['2023/15/03', '15-Mar-2023', '2023-03-15']

  [HIGH] monto → TREND_CHANGE
  Detalle   : Media del último 20% del período es 3.2σ por debajo del histórico
  Afectados : 9,046 registros (20.0%)
  Muestra   : [12.5, 8.3, 15.1, 9.7, 11.2]

  [HIGH] cliente_id → ID_DUPLICATES
  Detalle   : 847 IDs duplicados encontrados (1.87%)
  Afectados : 847 registros (1.87%)
  Muestra   : ['C-00123', 'C-00456', 'C-00789']
```
