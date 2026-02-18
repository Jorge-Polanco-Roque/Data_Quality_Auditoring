# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Quality Auditor — a Python framework that dynamically audits any CSV file without prior configuration. It uses classical statistics (no ML) to detect anomalies, outliers, nulls, format errors, PII, statistical drift, and business rule violations, then generates severity-classified reports in 7 formats. The full specification lives in `SkillDeCalidad.md`.

## Setup & Run

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Basic audit — generates all outputs in outputs/NNN_nombre/
python data_quality_auditor.py --input data.csv

# With schema validation and custom config
python data_quality_auditor.py --input data.csv --schema schema.yaml --config config.yaml

# Override specific output paths (still auto-generates the rest in outputs/)
python data_quality_auditor.py --input data.csv --output custom.json --excel-report custom.xlsx

# Disable auto-output folder (only explicit paths used)
python data_quality_auditor.py --input data.csv --no-auto-output --output report.json

# Batch mode: process all CSVs in a directory
python data_quality_auditor.py --batch ./data_dir

# Drift detection: compare current vs reference
python data_quality_auditor.py --input current.csv --compare reference.csv

# Filter severity, specify date column, silent mode
python data_quality_auditor.py --input data.csv --min-severity HIGH --date-col fecha --quiet

# Run tests
python -m pytest tests/ -v
```

Exit codes: 0=OK, 1=issues found, 2=critical issues.

## Output Organization

Every run auto-generates all report formats inside a serial-numbered folder:

```
outputs/
├── 001_test_iris/
│   ├── report.json           # Full structured report
│   ├── report.md             # Markdown with charts
│   ├── report.html           # Interactive HTML with Chart.js
│   ├── report.txt            # Plain text
│   ├── report.xlsx           # Excel with formatted tabs
│   ├── executive_summary.md  # 1-page stakeholder report
│   └── flagged_rows.csv      # Problematic rows with reasons
├── 002_test_flights/
│   └── ...
└── 003_drift_current/
    └── drift_report.json
```

- `NNN_<csv_basename>` auto-increments (001, 002, ...)
- Explicit flags (`--output`, `--md-report`, `--html-report`, `--excel-report`, `--text-report`) override the auto path
- `--no-auto-output` disables the auto-output folder entirely

## Architecture

6-layer sequential pipeline with dataset-level checks: CSV → DataLoader → TypeDetector → CheckRegistry → CheckEngine (per-column + dataset-level) → ScoringSystem → ReportBuilder → Reports (7 formats).

```
├── data_quality_auditor.py         # CLI entry point (argparse, all modes, auto-output)
├── generate_report_md.py           # Dynamic Markdown report generator
├── generate_report_html.py         # Interactive HTML report with Chart.js
├── generate_report_executive.py    # 1-page executive summary
├── generate_report_excel.py        # Excel with formatted tabs (openpyxl)
├── outputs/                        # Auto-generated run folders (NNN_nombre/)
├── core/
│   ├── data_loader.py              # Layer 1: encoding/delimiter detection, binary detection, large file sampling
│   ├── type_detector.py            # Layer 2: assigns 1 of 13 SemanticTypes per column
│   ├── check_registry.py           # Layer 3: declarative map of checks → semantic types
│   ├── check_engine.py             # Layer 4: per-column + dataset-level checks, config-aware
│   ├── scoring_system.py           # Layer 5: configurable health scores (100-point, A-F grades)
│   ├── report_builder.py           # Layer 6: JSON + text + profiling output
│   ├── schema_validator.py         # YAML schema validation (types, ranges, patterns, composite keys)
│   ├── config_loader.py            # YAML config for thresholds, disabled checks, severity overrides, weights
│   ├── batch_processor.py          # Multi-CSV batch processing with summary reports
│   ├── drift_detector.py           # Dataset comparison: schema diff, KS test, distribution shifts
│   ├── business_rules.py           # Conditional business rules engine (YAML DSL)
│   ├── flagged_rows.py             # Export problematic rows to CSV
│   ├── trend_analyzer.py           # Historical quality trend comparison across runs
│   └── referential_integrity.py    # Foreign key validation between CSVs
├── checks/                         # ~70+ checks organized by data type
│   ├── universal_checks.py         # NULL_RATE, DUPLICATE_ROWS, WHITESPACE, CONSTANT, NEAR_CONSTANT
│   ├── numeric_checks.py           # OUTLIER_IQR/ZSCORE/MODIFIED_Z, SKEW, KURTOSIS, TREND_CHANGE, etc.
│   ├── date_checks.py              # FORMAT_MIX, FUTURE, ANCIENT, GAPS, TEMPORAL_DRIFT, etc.
│   ├── categorical_checks.py       # RARE_CATEGORIES, TYPO_CANDIDATES, CASE_INCONSISTENCY, etc.
│   ├── text_checks.py              # EMAIL_FORMAT, PHONE_FORMAT, LENGTH_OUTLIERS, etc.
│   ├── id_checks.py                # ID_DUPLICATES, ID_FORMAT_CONSISTENCY, ID_NULL
│   ├── hypothesis_checks.py        # Parametric/non-parametric hypothesis tests (adaptive)
│   ├── benford_check.py            # Benford's Law first-digit analysis
│   ├── cross_column_checks.py      # Correlation matrix, VIF, Cramér's V, point-biserial
│   ├── null_pattern_checks.py      # MCAR violation, null correlation, row patterns
│   ├── timeseries_checks.py        # Autocorrelation, seasonality (STL), changepoints (CUSUM)
│   ├── pii_checks.py               # PII detection: emails, phones, credit cards, SSN, CURP, RFC, IPs
│   └── temporal_completeness_checks.py  # Null rate by period, degradation detection
├── models/
│   ├── check_result.py             # CheckResult dataclass
│   └── semantic_type.py            # SemanticType enum (13 types)
├── tests/                          # pytest unit tests (89 tests)
└── requirements.txt
```

## Key Design Decisions

- **TypeDetector uses priority-based detection**: first match wins in order: EMPTY → CONSTANT → BOOLEAN → numeric subtypes → date/email/phone patterns → cardinality-based classification → MIXED
- **Dual DataFrame approach**: `df_raw` (all strings, for format inspection) and `df` (pandas-inferred types) are produced in parallel by DataLoader
- **Fail-safe checks**: every check runs inside try/except; failures produce INFO-severity results, never crash the pipeline
- **Configurable scoring**: column score starts at 100, deducts per issue (defaults: CRITICAL=-25, HIGH=-10, MEDIUM=-5, LOW=-2). All deductions and column weights customizable via YAML config
- **Two levels of checks**: per-column checks (mapped via CheckRegistry) and dataset-level checks (cross-column, null patterns, timeseries, PII, temporal completeness)
- **Config-aware engine**: checks can be disabled, severities overridden, and scoring weights adjusted per column via YAML config
- **Robustness**: binary file detection, header-only file detection, large file sampling (>500MB), column limit (500), bad line skipping

## Config System

Pass `--config config.yaml` to customize:

```yaml
disabled_checks:
  - NORMALITY_ANDERSON
  - BENFORD_LAW
severity_overrides:
  MEAN_SHIFT: LOW
  ADF_STATIONARITY: INFO
thresholds:
  NULL_RATE:
    CRITICAL: 0.50
    HIGH: 0.20
scoring:                    # Override severity deductions
  CRITICAL: 30
  HIGH: 15
column_weights:             # Override column importance (default: 1/(1+null_pct))
  amount: 5.0
  customer_id: 3.0
business_rules:             # Conditional inter-column rules
  - name: "Refund requires cancellation"
    condition: "status == 'cancelled'"
    assertion: "refund_amount > 0"
    severity: HIGH
foreign_keys:               # Referential integrity (batch mode)
  - child_table: orders.csv
    child_column: customer_id
    parent_table: customers.csv
    parent_column: id
```

## Schema Validation

Pass `--schema schema.yaml`:

```yaml
columns:
  column_name:
    type: numeric|categorical|date|text|boolean|email|phone
    required: true/false
    not_null: true/false
    unique: true/false
    min: 0
    max: 100
    allowed_values: [a, b, c]
    pattern: "^[A-Z]{3}-\\d+$"
composite_keys:
  - [col1, col2]
```

## Dependencies

pandas, numpy, scipy, chardet, rapidfuzz, python-dateutil, pymannkendall, rich, statsmodels, pyyaml, openpyxl, pytest.

## Language

The specification (`SkillDeCalidad.md`) and report templates are in Spanish. Code identifiers and docstrings may be in either English or Spanish — follow the existing convention in each file.
