from dataclasses import dataclass, field
from typing import List, Dict, Any


@dataclass
class CheckResult:
    check_id: str
    column: str
    passed: bool
    severity: str  # CRITICAL | HIGH | MEDIUM | LOW | INFO | PASS
    value: float
    threshold: float
    message: str
    affected_count: int = 0
    affected_pct: float = 0.0
    sample_values: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_id": self.check_id,
            "column": self.column,
            "passed": self.passed,
            "severity": self.severity,
            "value": self.value,
            "threshold": self.threshold,
            "message": self.message,
            "affected_count": self.affected_count,
            "affected_pct": round(self.affected_pct, 6),
            "sample_values": [str(v) for v in self.sample_values],
            "metadata": self.metadata,
        }
