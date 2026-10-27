"""Pydantic data models for runtime engine verification and rule-vs-runtime analysis."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, computed_field


class RuntimeVerdict(str, Enum):
    """Runtime execution verdict from a query/compute engine."""

    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    NOT_RUN = "NOT_RUN"
    UNSUPPORTED = "UNSUPPORTED"
    ERROR = "ERROR"


class MetadataVerdict(str, Enum):
    """Static metadata-level compatibility prediction."""

    COMPATIBLE = "COMPATIBLE"
    INCOMPATIBLE = "INCOMPATIBLE"
    WARNING = "WARNING"
    UNKNOWN = "UNKNOWN"
    ERROR = "ERROR"


class ValidationScope(str, Enum):
    """Scope and authorization boundary of data validation."""

    METADATA_ONLY = "METADATA_ONLY"
    SYNTHETIC_DATA = "SYNTHETIC_DATA"
    USER_AUTHORIZED_DATA = "USER_AUTHORIZED_DATA"


class EngineProbeOperation(str, Enum):
    """Type of runtime probe executed on target engine."""

    READ = "READ"
    CAST = "CAST"
    SCHEMA_UNION = "SCHEMA_UNION"
    FILTER = "FILTER"


class DisagreementClassification(str, Enum):
    """Classification of divergence between rule prediction and runtime observation."""

    AGREEMENT = "AGREEMENT"
    RULE_CONSERVATIVE = "RULE_CONSERVATIVE"  # Rule predicted break/warning, but engine succeeded
    RULE_OPTIMISTIC = "RULE_OPTIMISTIC"  # Rule predicted safe, but engine failed (hazardous!)
    INCONCLUSIVE = "INCONCLUSIVE"


class EngineProbeResult(BaseModel):
    """Result of a single probe operation on a specific engine."""

    engine_name: str
    engine_version: Optional[str] = None
    operation: EngineProbeOperation
    runtime_verdict: RuntimeVerdict
    duration_ms: float = 0.0
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    executed_expression: Optional[str] = None
    tested_invariants: List[str] = Field(default_factory=list)
    details: Dict[str, Any] = Field(default_factory=dict)


class RuleVsRuntimeDisagreement(BaseModel):
    """A structured discrepancy where static rules and runtime execution diverged."""

    engine_name: str
    field_name: Optional[str] = None
    rule_predicted_verdict: MetadataVerdict
    runtime_verdict: RuntimeVerdict
    classification: DisagreementClassification
    finding_id: Optional[str] = None
    explanation: str
    remediation_advice: Optional[str] = None


class EngineVerificationReport(BaseModel):
    """Comprehensive multi-engine verification and discrepancy report."""

    target_path: str
    validation_scope: ValidationScope = ValidationScope.SYNTHETIC_DATA
    timestamp_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata_verdict: MetadataVerdict = MetadataVerdict.COMPATIBLE
    runtime_verdict: RuntimeVerdict = RuntimeVerdict.PASS
    engine_results: List[EngineProbeResult] = Field(default_factory=list)
    disagreements: List[RuleVsRuntimeDisagreement] = Field(default_factory=list)
    finding_ids: List[str] = Field(default_factory=list)
    tested_invariants: List[str] = Field(default_factory=list)
    limitations: List[str] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def has_rule_optimistic_disagreement(self) -> bool:
        """Return True if any rule falsely predicted safety when engine runtime failed."""
        return any(d.classification == DisagreementClassification.RULE_OPTIMISTIC for d in self.disagreements)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def overall_compatible(self) -> bool:
        """Return True if both metadata is non-breaking and all executed runtime probes passed."""
        meta_ok = self.metadata_verdict in (MetadataVerdict.COMPATIBLE, MetadataVerdict.WARNING)
        runtime_ok = all(
            r.runtime_verdict in (RuntimeVerdict.PASS, RuntimeVerdict.NOT_RUN) for r in self.engine_results
        )
        return meta_ok and runtime_ok and not self.has_rule_optimistic_disagreement
