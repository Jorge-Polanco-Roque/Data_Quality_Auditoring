"""
Batch processor: procesa múltiples CSVs de un directorio y genera reporte consolidado.
"""

import os
import json
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from generate_report_md import generate_markdown


def _process_single_file(file_path, args_dict, schema, config):
    """Worker function para procesamiento paralelo."""
    # Importaciones dentro del worker para compatibilidad con multiprocessing
    from core.data_loader import DataLoader
    from core.type_detector import TypeDetector
    from core.check_engine import CheckEngine
    from core.scoring_system import ScoringSystem
    from core.report_builder import ReportBuilder

    try:
        loader = DataLoader()
        df_raw, df, metadata = loader.load(file_path)

        detector = TypeDetector()
        column_types = detector.detect(df_raw, df)

        engine = CheckEngine(config=config)
        results = engine.run_all(df_raw, df, column_types, date_col=args_dict.get("date_col"))

        if schema:
            from core.schema_validator import SchemaValidator
            schema_results = SchemaValidator(schema).validate(df_raw, df, column_types)
            results.extend(schema_results)

        null_pcts = {col: float(df[col].isna().mean()) for col in df.columns}
        scorer = ScoringSystem()
        scoring = scorer.calculate(results, null_pcts)

        builder = ReportBuilder()
        report = builder.build(results, scoring, column_types, metadata, df)

        return {"file": file_path, "status": "ok", "report": report, "scoring": scoring}
    except Exception as e:
        return {"file": file_path, "status": "error", "error": str(e)}


class BatchProcessor:
    """Procesa todos los CSVs de un directorio."""

    def __init__(self, args, schema=None, config=None):
        self.args = args
        self.schema = schema
        self.config = config
        self.quiet = getattr(args, "quiet", False)

    def run(self, directory: str):
        csv_files = sorted(glob.glob(os.path.join(directory, "*.csv")))
        if not csv_files:
            print(f"No se encontraron archivos CSV en: {directory}")
            return

        if not self.quiet:
            print(f"Procesando {len(csv_files)} archivos CSV de: {directory}")
            print()

        args_dict = {"date_col": getattr(self.args, "date_col", None)}
        results = []

        # Procesar secuencialmente (más seguro con pandas/numpy)
        for i, csv_file in enumerate(csv_files, 1):
            if not self.quiet:
                print(f"  [{i}/{len(csv_files)}] {os.path.basename(csv_file)}...", end=" ", flush=True)

            result = _process_single_file(csv_file, args_dict, self.schema, self.config)
            results.append(result)

            if not self.quiet:
                if result["status"] == "ok":
                    score = result["report"]["dataset_summary"]["health_score"]
                    grade = result["report"]["dataset_summary"]["health_grade"]
                    print(f"Score: {score}/100 ({grade})")
                else:
                    print(f"ERROR: {result['error']}")

        # Generar reportes individuales
        output_dir = getattr(self.args, "output", None) or os.path.join(directory, "reports")
        os.makedirs(output_dir, exist_ok=True)

        for result in results:
            if result["status"] != "ok":
                continue
            base = os.path.splitext(os.path.basename(result["file"]))[0]

            # JSON
            json_path = os.path.join(output_dir, f"{base}_report.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(result["report"], f, ensure_ascii=False, indent=2, default=str)

            # Markdown
            md_path = os.path.join(output_dir, f"{base}_report.md")
            md = generate_markdown(result["report"])
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(md)

        # Reporte consolidado
        summary = self._build_summary(results)
        summary_path = os.path.join(output_dir, "batch_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

        # Markdown consolidado
        md_summary = self._build_summary_md(summary)
        md_summary_path = os.path.join(output_dir, "batch_summary.md")
        with open(md_summary_path, "w", encoding="utf-8") as f:
            f.write(md_summary)

        if not self.quiet:
            print(f"\nReportes guardados en: {output_dir}")
            print(f"  Archivos procesados: {summary['total_files']}")
            print(f"  Exitosos: {summary['successful']}")
            print(f"  Errores: {summary['errors']}")
            print(f"  Score promedio: {summary['avg_score']}/100")

    def _build_summary(self, results):
        ok_results = [r for r in results if r["status"] == "ok"]
        err_results = [r for r in results if r["status"] == "error"]

        scores = [r["report"]["dataset_summary"]["health_score"] for r in ok_results]
        avg_score = round(sum(scores) / len(scores), 1) if scores else 0.0

        files_detail = []
        for r in results:
            if r["status"] == "ok":
                files_detail.append({
                    "file": os.path.basename(r["file"]),
                    "score": r["report"]["dataset_summary"]["health_score"],
                    "grade": r["report"]["dataset_summary"]["health_grade"],
                    "issues": r["report"]["dataset_summary"]["total_issues"],
                    "rows": r["report"]["report_metadata"]["total_rows"],
                    "columns": r["report"]["report_metadata"]["total_columns"],
                })
            else:
                files_detail.append({
                    "file": os.path.basename(r["file"]),
                    "score": None,
                    "grade": "ERROR",
                    "error": r["error"],
                })

        return {
            "generated_at": datetime.now().isoformat(),
            "total_files": len(results),
            "successful": len(ok_results),
            "errors": len(err_results),
            "avg_score": avg_score,
            "min_score": min(scores) if scores else 0.0,
            "max_score": max(scores) if scores else 0.0,
            "files": files_detail,
        }

    def _build_summary_md(self, summary):
        lines = [
            "# Batch Data Quality Report",
            "",
            f"> Generado el **{summary['generated_at'][:10]}**",
            "",
            "## Resumen",
            "",
            f"| Métrica | Valor |",
            f"|---------|-------|",
            f"| Archivos procesados | {summary['total_files']} |",
            f"| Exitosos | {summary['successful']} |",
            f"| Errores | {summary['errors']} |",
            f"| Score promedio | **{summary['avg_score']}/100** |",
            f"| Score mínimo | {summary['min_score']}/100 |",
            f"| Score máximo | {summary['max_score']}/100 |",
            "",
            "## Detalle por Archivo",
            "",
            "| Archivo | Score | Grado | Issues | Filas | Columnas |",
            "|---------|-------|-------|--------|-------|----------|",
        ]

        for f in summary["files"]:
            if f.get("score") is not None:
                lines.append(
                    f"| `{f['file']}` | {f['score']} | {f['grade']} | "
                    f"{f.get('issues', 0)} | {f.get('rows', 0):,} | {f.get('columns', 0)} |"
                )
            else:
                lines.append(f"| `{f['file']}` | - | ERROR | {f.get('error', '')[:50]} | - | - |")

        lines.extend(["", "---", ""])
        return "\n".join(lines)
