import os
import pandas as pd
import chardet


DELIMITERS = [",", ";", "\t", "|"]
MAX_COLUMNS = 500
LARGE_FILE_MB = 500  # Archivos > 500MB usan sampling


class DataLoader:
    """Capa 1: Carga robusta de CSV con detección automática de encoding y delimiter."""

    def load(self, file_path: str):
        """Carga un CSV y retorna (df_raw, df, metadata).

        df_raw: todos los valores como strings (para inspección de formato).
        df: con tipos inferidos por pandas.
        metadata: dict con n_rows, n_cols, file_size_mb, encoding, delimiter.
        """
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

        # Verificar que no es binario
        self._check_not_binary(file_path)

        encoding = self._detect_encoding(file_path)
        delimiter = self._detect_delimiter(file_path, encoding)

        # Archivos grandes: sampling
        read_kwargs = {}
        sampled = False
        if file_size_mb > LARGE_FILE_MB:
            # Estimar filas y samplear
            with open(file_path, "r", encoding=encoding, errors="replace") as f:
                for i, _ in enumerate(f):
                    if i > 100_000:
                        break
            if i > 100_000:
                read_kwargs["nrows"] = 100_000
                sampled = True

        df_raw = pd.read_csv(
            file_path,
            sep=delimiter,
            encoding=encoding,
            dtype=str,
            keep_default_na=False,
            on_bad_lines="skip",
            **read_kwargs,
        )

        df = pd.read_csv(
            file_path,
            sep=delimiter,
            encoding=encoding,
            on_bad_lines="skip",
            **read_kwargs,
        )

        # Limitar columnas para performance
        if len(df.columns) > MAX_COLUMNS:
            df_raw = df_raw.iloc[:, :MAX_COLUMNS]
            df = df.iloc[:, :MAX_COLUMNS]

        # Verificar que no es header-only
        if len(df) == 0:
            raise ValueError(f"Archivo sin datos (solo headers): {file_path}")

        metadata = {
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "n_rows": len(df),
            "n_cols": len(df.columns),
            "file_size_mb": round(file_size_mb, 3),
            "encoding": encoding,
            "delimiter": repr(delimiter),
            "sampled": sampled,
        }

        return df_raw, df, metadata

    def _check_not_binary(self, file_path: str):
        """Detecta si un archivo es binario (no texto)."""
        with open(file_path, "rb") as f:
            chunk = f.read(8192)
        # Bytes nulos indican archivo binario
        null_count = chunk.count(b'\x00')
        if null_count > len(chunk) * 0.1:
            raise ValueError(f"El archivo parece ser binario, no un CSV: {file_path}")

    def _detect_encoding(self, file_path: str) -> str:
        sample_size = min(os.path.getsize(file_path), 100_000)
        with open(file_path, "rb") as f:
            raw = f.read(sample_size)
        result = chardet.detect(raw)
        encoding = result.get("encoding", "utf-8") or "utf-8"
        # Normalizar variantes comunes
        if encoding.lower() in ("ascii", "windows-1252", "iso-8859-1"):
            encoding = "latin-1"
        return encoding

    def _detect_delimiter(self, file_path: str, encoding: str) -> str:
        with open(file_path, "r", encoding=encoding, errors="replace") as f:
            sample_lines = []
            for i, line in enumerate(f):
                if i >= 20:
                    break
                sample_lines.append(line)

        if not sample_lines:
            return ","

        best_delimiter = ","
        best_cols = 0

        for delim in DELIMITERS:
            cols_per_line = [len(line.split(delim)) for line in sample_lines if line.strip()]
            if not cols_per_line:
                continue
            # Usar la moda de columnas (la más frecuente)
            avg_cols = max(set(cols_per_line), key=cols_per_line.count)
            if avg_cols > best_cols:
                best_cols = avg_cols
                best_delimiter = delim

        return best_delimiter
