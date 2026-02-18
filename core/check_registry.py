from typing import List, Dict, Callable

from models.semantic_type import SemanticType
from checks.universal_checks import UNIVERSAL_CHECKS
from checks.numeric_checks import NUMERIC_CHECKS
from checks.date_checks import DATE_CHECKS
from checks.categorical_checks import CATEGORICAL_CHECKS
from checks.text_checks import TEXT_CHECKS_GENERIC, EMAIL_CHECKS, PHONE_CHECKS
from checks.id_checks import ID_CHECKS
from checks.hypothesis_checks import HYPOTHESIS_NUMERIC_CHECKS, HYPOTHESIS_CATEGORICAL_CHECKS
from checks.benford_check import BENFORD_CHECKS


# Mapeo de checks a tipos semánticos
TYPE_CHECK_MAP: Dict[SemanticType, List[dict]] = {
    # Numéricos
    SemanticType.NUMERIC_CONTINUOUS: UNIVERSAL_CHECKS + NUMERIC_CHECKS + HYPOTHESIS_NUMERIC_CHECKS + BENFORD_CHECKS,
    SemanticType.NUMERIC_DISCRETE: UNIVERSAL_CHECKS + NUMERIC_CHECKS + HYPOTHESIS_NUMERIC_CHECKS + BENFORD_CHECKS,

    # Fechas
    SemanticType.DATE: UNIVERSAL_CHECKS + DATE_CHECKS,
    SemanticType.DATETIME: UNIVERSAL_CHECKS + DATE_CHECKS,

    # Categóricos
    SemanticType.CATEGORICAL: UNIVERSAL_CHECKS + CATEGORICAL_CHECKS + HYPOTHESIS_CATEGORICAL_CHECKS,
    SemanticType.BOOLEAN: UNIVERSAL_CHECKS + CATEGORICAL_CHECKS + HYPOTHESIS_CATEGORICAL_CHECKS,

    # Texto / Alta cardinalidad
    SemanticType.HIGH_CARDINALITY: UNIVERSAL_CHECKS + TEXT_CHECKS_GENERIC,
    SemanticType.EMAIL: UNIVERSAL_CHECKS + TEXT_CHECKS_GENERIC + EMAIL_CHECKS,
    SemanticType.PHONE: UNIVERSAL_CHECKS + TEXT_CHECKS_GENERIC + PHONE_CHECKS,

    # IDs
    SemanticType.ID_CANDIDATE: UNIVERSAL_CHECKS + ID_CHECKS,

    # Especiales
    SemanticType.MIXED: UNIVERSAL_CHECKS,
    SemanticType.EMPTY: UNIVERSAL_CHECKS,
    SemanticType.CONSTANT: UNIVERSAL_CHECKS,
}


class CheckRegistry:
    """Capa 3: Mapa declarativo de qué checks aplican a cada tipo semántico."""

    def get_checks_for_type(self, semantic_type: SemanticType) -> List[dict]:
        """Retorna la lista de checks aplicables a un tipo semántico."""
        return TYPE_CHECK_MAP.get(semantic_type, UNIVERSAL_CHECKS)

    def get_all_check_ids(self) -> List[str]:
        """Retorna todos los check_ids registrados (sin duplicados)."""
        seen = set()
        ids = []
        for checks in TYPE_CHECK_MAP.values():
            for check in checks:
                cid = check["check_id"]
                if cid not in seen:
                    seen.add(cid)
                    ids.append(cid)
        return ids
