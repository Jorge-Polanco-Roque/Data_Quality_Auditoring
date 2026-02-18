#!/usr/bin/env python3
"""Data Quality Auditor — Auditoría dinámica de calidad de datos para cualquier CSV."""

import os
import sys
import glob
import json
import argparse
from datetime import datetime

from core.data_loader import DataLoader
from core.type_detector import TypeDetector
from core.check_engine import CheckEngine
from core.scoring_system import ScoringSystem
from core.report_builder import ReportBuilder
from generate_report_md import generate_markdown


OUTPUTS_DIR = "outputs"


def _next_run_dir(base_name: str) -> str:
    """Crea y retorna el directorio de la siguiente corrida: outputs/NNN_nombre/"""
    os.makedirs(OUTPUTS_DIR, exist_ok=True)

    existing = glob.glob(os.path.join(OUTPUTS_DIR, "[0-9][0-9][0-9]_*"))
    if existing:
        nums = []
        for d in existing:
            folder = os.path.basename(d)
            try:
                nums.append(int(folder[:3]))
            except ValueError:
                pass
        next_num = max(nums) + 1 if nums else 1
    else:
        next_num = 1

    run_dir = os.path.join(OUTPUTS_DIR, f"{next_num:03d}_{base_name}")
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def audit_single(input_path, args, schema=None, config=None):
    """Ejecuta auditoría completa sobre un solo CSV. Retorna (report, scoring, df_raw, df, column_types, results)."""
    # Capa 1: Cargar datos
    loader = DataLoader()
    df_raw, df, metadata = loader.load(input_path)

    if not args.quiet:
        print(f"Archivo cargado: {metadata['file_name']}")
        print(f"  Filas: {metadata['n_rows']:,} | Columnas: {metadata['n_cols']} | "
              f"Encoding: {metadata['encoding']} | Delimiter: {metadata['delimiter']}")
        print()

    # Capa 2: Detectar tipos
    detector = TypeDetector()
    column_types = detector.detect(df_raw, df)

    if not args.quiet:
        print("Tipos semánticos detectados:")
        for col, stype in column_types.items():
            print(f"  {col}: {stype.value}")
        print()

    # Capa 3-4: Ejecutar checks
    engine = CheckEngine(config=config)
    results = engine.run_all(df_raw, df, column_types, date_col=args.date_col)

    # Schema validation
    if schema:
        from core.schema_validator import SchemaValidator
        schema_results = SchemaValidator(schema).validate(df_raw, df, column_types)
        results.extend(schema_results)

    # Business rules
    if config and config.get("business_rules"):
        from core.business_rules import BusinessRulesEngine
        br_engine = BusinessRulesEngine(config["business_rules"])
        results.extend(br_engine.evaluate(df))

    # Capa 5: Scoring (configurable)
    null_pcts = {}
    for col in df.columns:
        null_pcts[col] = float(df[col].isna().mean())

    scorer = ScoringSystem(config=config)
    scoring = scorer.calculate(results, null_pcts)

    # Capa 6: Reporte
    builder = ReportBuilder()
    report = builder.build(results, scoring, column_types, metadata, df)

    return report, scoring, df_raw, df, column_types, results


def main():
    parser = argparse.ArgumentParser(
        description="Data Quality Auditor: auditoría automática de calidad para CSVs"
    )
    parser.add_argument("--input", help="Ruta al archivo CSV")
    parser.add_argument("--output", help="Ruta para el reporte JSON (overrides carpeta automática)")
    parser.add_argument("--text-report", help="Ruta para el reporte de texto plano")
    parser.add_argument("--md-report", help="Ruta para el reporte Markdown (overrides carpeta automática)")
    parser.add_argument("--html-report", help="Ruta para el reporte HTML (overrides carpeta automática)")
    parser.add_argument("--excel-report", help="Ruta para el reporte Excel (overrides carpeta automática)")
    parser.add_argument(
        "--min-severity",
        choices=["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"],
        default="LOW",
        help="Severidad mínima a mostrar en stdout (default: LOW)",
    )
    parser.add_argument("--date-col", help="Nombre de la columna de fecha para análisis temporal")
    parser.add_argument("--quiet", action="store_true", help="Modo silencioso (solo exit code)")
    parser.add_argument("--schema", help="Ruta a archivo YAML de schema esperado")
    parser.add_argument("--config", help="Ruta a archivo YAML de configuración (umbrales, toggles)")
    parser.add_argument("--batch", help="Ruta a directorio para procesar todos los CSVs")
    parser.add_argument("--compare", help="Ruta a CSV de referencia para detección de drift")
    parser.add_argument("--no-auto-output", action="store_true",
                        help="No generar outputs automáticos en outputs/")

    args = parser.parse_args()

    # Validar argumentos
    if not args.input and not args.batch:
        parser.error("Se requiere --input o --batch")

    # Cargar config y schema si se proporcionan
    config = None
    if args.config:
        from core.config_loader import ConfigLoader
        config = ConfigLoader.load(args.config)

    schema = None
    if args.schema:
        import yaml
        with open(args.schema, "r", encoding="utf-8") as f:
            schema = yaml.safe_load(f)

    # ── Modo batch ──
    if args.batch:
        from core.batch_processor import BatchProcessor
        processor = BatchProcessor(args, schema=schema, config=config)
        processor.run(args.batch)
        return

    # ── Modo comparación / drift ──
    if args.compare:
        from core.drift_detector import DriftDetector
        detector = DriftDetector()
        drift_report = detector.compare(args.compare, args.input)

        if not args.no_auto_output and not args.output:
            base = os.path.splitext(os.path.basename(args.input))[0]
            run_dir = _next_run_dir(f"drift_{base}")
            out = os.path.join(run_dir, "drift_report.json")
        else:
            out = args.output or "drift_report.json"

        with open(out, "w", encoding="utf-8") as f:
            json.dump(drift_report, f, ensure_ascii=False, indent=2, default=str)
        if not args.quiet:
            print(f"Drift report guardado en: {out}")
            detector.print_summary(drift_report)
        return

    # ── Modo normal (un solo archivo) ──
    try:
        report, scoring, df_raw, df, column_types, results = audit_single(
            args.input, args, schema=schema, config=config
        )
    except Exception as e:
        print(f"Error al auditar archivo: {e}", file=sys.stderr)
        sys.exit(2)

    # ── Determinar carpeta de salida ──
    run_dir = None
    if not args.no_auto_output:
        base_name = os.path.splitext(os.path.basename(args.input))[0]
        run_dir = _next_run_dir(base_name)

    # ── Trend histórico ──
    try:
        from core.trend_analyzer import TrendAnalyzer
        trend_base = os.path.splitext(os.path.basename(args.input))[0]
        trend = TrendAnalyzer().build_trend_report(
            trend_base, scoring["dataset_score"], scoring["dataset_grade"]
        )
        if trend:
            report["quality_trend"] = trend
    except Exception:
        pass

    # ── Flagged rows ──
    flagged_df = None
    try:
        from core.flagged_rows import FlaggedRowsExporter
        exporter = FlaggedRowsExporter()
        flagged_df = exporter.collect_flagged_rows(df_raw, df, results, column_types)
    except Exception:
        pass

    # ── Generar outputs ──
    builder = ReportBuilder()

    # JSON
    json_path = args.output
    if not json_path and run_dir:
        json_path = os.path.join(run_dir, "report.json")
    if json_path:
        builder.to_json(report, json_path)
        if not args.quiet:
            print(f"Reporte JSON guardado en: {json_path}")

    # Markdown
    md_path = args.md_report
    if not md_path and run_dir:
        md_path = os.path.join(run_dir, "report.md")
    if md_path:
        md = generate_markdown(report)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md)
        if not args.quiet:
            print(f"Reporte Markdown guardado en: {md_path}")

    # HTML
    html_path = args.html_report
    if not html_path and run_dir:
        html_path = os.path.join(run_dir, "report.html")
    if html_path:
        from generate_report_html import generate_html
        html = generate_html(report)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        if not args.quiet:
            print(f"Reporte HTML guardado en: {html_path}")

    # Executive summary
    if run_dir:
        try:
            from generate_report_executive import generate_executive_summary
            exec_md = generate_executive_summary(report)
            exec_path = os.path.join(run_dir, "executive_summary.md")
            with open(exec_path, "w", encoding="utf-8") as f:
                f.write(exec_md)
            if not args.quiet:
                print(f"Resumen ejecutivo guardado en: {exec_path}")
        except Exception:
            pass

    # Excel
    excel_path = args.excel_report
    if not excel_path and run_dir:
        excel_path = os.path.join(run_dir, "report.xlsx")
    if excel_path:
        try:
            from generate_report_excel import generate_excel
            generate_excel(report, excel_path, flagged_df=flagged_df)
            if not args.quiet:
                print(f"Reporte Excel guardado en: {excel_path}")
        except ImportError:
            if not args.quiet:
                print("Nota: instalar openpyxl para generar reportes Excel (pip install openpyxl)")
        except Exception:
            pass

    # Flagged rows CSV
    if run_dir and flagged_df is not None and len(flagged_df) > 0:
        try:
            flagged_path = os.path.join(run_dir, "flagged_rows.csv")
            flagged_df.to_csv(flagged_path, index=False, encoding="utf-8")
            if not args.quiet:
                print(f"Filas flaggeadas guardado en: {flagged_path} ({len(flagged_df):,} flags)")
        except Exception:
            pass

    # Texto
    txt_path = args.text_report
    if not txt_path and run_dir:
        txt_path = os.path.join(run_dir, "report.txt")
    text_report = builder.to_text(report, output_path=txt_path)

    if not args.quiet:
        print()
        print(text_report)

    if txt_path and not args.quiet:
        print(f"\nReporte de texto guardado en: {txt_path}")

    if run_dir and not args.quiet:
        print(f"\n{'='*50}")
        print(f"  Todos los outputs en: {run_dir}/")
        print(f"{'='*50}")

    # Exit code
    if scoring["issues_by_severity"]["CRITICAL"] > 0:
        sys.exit(2)
    elif scoring["total_issues"] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
