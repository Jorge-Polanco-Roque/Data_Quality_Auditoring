import re
from typing import Dict

import numpy as np
import pandas as pd
from dateutil import parser as date_parser

from models.semantic_type import SemanticType


BOOLEAN_VALUES = {
    "true", "false", "t", "f",
    "yes", "no", "y", "n",
    "si", "no", "sí",
    "1", "0",
    "verdadero", "falso",
}

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

PHONE_RE = re.compile(
    r"^[\+]?[\d\s\-\.\(\)]{7,20}$"
)

DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
    "%Y/%m/%d", "%d.%m.%Y", "%Y%m%d",
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
    "%d %b %Y", "%B %d, %Y", "%d de %B de %Y",
]

DATETIME_FORMATS = {
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
}


class TypeDetector:
    """Capa 2: Infiere el tipo semántico de cada columna."""

    def detect(self, df_raw: pd.DataFrame, df: pd.DataFrame) -> Dict[str, SemanticType]:
        column_types = {}
        for col in df_raw.columns:
            column_types[col] = self._detect_column(df_raw[col], df[col] if col in df.columns else df_raw[col])
        return column_types

    def _detect_column(self, series_raw: pd.Series, series_typed: pd.Series) -> SemanticType:
        n_rows = len(series_raw)
        if n_rows == 0:
            return SemanticType.EMPTY

        # Trabajar con la serie raw (strings) para inspección
        stripped = series_raw.astype(str).str.strip()
        non_empty_mask = (stripped != "") & (stripped.str.lower() != "nan")
        n_nonnull = non_empty_mask.sum()
        null_pct = 1.0 - (n_nonnull / n_rows) if n_rows > 0 else 1.0

        # 1. EMPTY
        if null_pct >= 0.95:
            return SemanticType.EMPTY

        non_empty = stripped[non_empty_mask]
        n_unique = non_empty.nunique()

        # 2. CONSTANT
        if n_unique == 1:
            return SemanticType.CONSTANT

        # 3. BOOLEAN
        if n_unique == 2:
            vals = set(non_empty.str.lower().unique())
            if vals.issubset(BOOLEAN_VALUES):
                return SemanticType.BOOLEAN

        # 4. Numérico (si pandas lo detectó como numérico)
        if pd.api.types.is_numeric_dtype(series_typed):
            ratio = n_unique / n_nonnull if n_nonnull > 0 else 0
            if ratio < 0.05:
                return SemanticType.NUMERIC_DISCRETE
            return SemanticType.NUMERIC_CONTINUOUS

        # 5. Object/string: intentar patrones
        sample_size = min(200, len(non_empty))
        sample = non_empty.sample(n=sample_size, random_state=42) if len(non_empty) > sample_size else non_empty

        # 5a. Fechas
        is_datetime, date_match_pct = self._check_dates(sample)
        if date_match_pct > 0.80:
            return SemanticType.DATETIME if is_datetime else SemanticType.DATE

        # 5b. Email
        email_pct = sample.apply(lambda x: bool(EMAIL_RE.match(str(x)))).mean()
        if email_pct > 0.80:
            return SemanticType.EMAIL

        # 5c. Phone
        phone_pct = sample.apply(lambda x: bool(PHONE_RE.match(str(x)))).mean()
        if phone_pct > 0.80:
            return SemanticType.PHONE

        # 5d-f. Cardinalidad
        ratio = n_unique / n_nonnull if n_nonnull > 0 else 0
        if ratio > 0.85:
            # Distinguir ID_CANDIDATE de HIGH_CARDINALITY
            if self._looks_like_id(sample):
                return SemanticType.ID_CANDIDATE
            return SemanticType.HIGH_CARDINALITY
        if ratio < 0.15:
            return SemanticType.CATEGORICAL

        return SemanticType.HIGH_CARDINALITY

    def _check_dates(self, sample: pd.Series):
        """Intenta parsear la muestra como fecha. Retorna (is_datetime, match_pct)."""
        parsed = 0
        is_datetime = False

        for val in sample:
            val_str = str(val).strip()
            if not val_str:
                continue
            matched_fmt = None
            for fmt in DATE_FORMATS:
                try:
                    from datetime import datetime
                    datetime.strptime(val_str, fmt)
                    matched_fmt = fmt
                    break
                except ValueError:
                    continue

            if matched_fmt is None:
                try:
                    date_parser.parse(val_str, fuzzy=False)
                    matched_fmt = "dateutil"
                except (ValueError, OverflowError):
                    continue

            if matched_fmt:
                parsed += 1
                if matched_fmt in DATETIME_FORMATS or (matched_fmt == "dateutil" and " " in val_str):
                    is_datetime = True

        match_pct = parsed / len(sample) if len(sample) > 0 else 0
        return is_datetime, match_pct

    def _looks_like_id(self, sample: pd.Series) -> bool:
        """Heurística: IDs suelen tener un patrón estructurado consistente."""
        id_patterns = [
            re.compile(r"^[A-Fa-f0-9\-]{8,}$"),  # UUID-like
            re.compile(r"^[A-Z]{1,5}[\-_]\d+$"),  # PREFIX-123
            re.compile(r"^\d{5,}$"),               # Números largos
            re.compile(r"^[A-Z0-9]{6,}$"),         # Códigos alfanuméricos
        ]
        for pattern in id_patterns:
            match_pct = sample.astype(str).apply(lambda x: bool(pattern.match(x))).mean()
            if match_pct > 0.70:
                return True
        return False
