import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Type
from .exceptions import PayloadValidationError


@dataclass
class StageRecord:
    """Audit trail record for a single stage execution within a task."""

    stage_name: str
    started_at: str
    finished_at: str
    attempt: int
    status: str


@dataclass
class ExecutionContext:
    """Structured runtime metadata passed alongside task execution."""

    task_id: str
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    metadata: Dict[str, Any] = field(default_factory=dict)
    history: List[StageRecord] = field(default_factory=list)

    def record_stage(
        self,
        stage_name: str,
        started_at: datetime,
        finished_at: datetime,
        attempt: int,
        status: str,
    ) -> None:
        self.history.append(
            StageRecord(
                stage_name=stage_name,
                started_at=started_at.isoformat(),
                finished_at=finished_at.isoformat(),
                attempt=attempt,
                status=status,
            )
        )


class PayloadValidator:
    """Validates stage payload keys, types, and JSON serializability."""

    def __init__(
        self,
        required_keys: Optional[Iterable[str]] = None,
        field_types: Optional[Dict[str, Type[Any]]] = None,
        enforce_json_serializable: bool = True,
    ) -> None:
        self.required_keys = list(required_keys) if required_keys else []
        self.field_types = field_types or {}
        self.enforce_json_serializable = enforce_json_serializable

    def validate(self, payload: Dict[str, Any], stage_name: str = "<unknown>") -> None:
        if not isinstance(payload, dict):
            raise PayloadValidationError(
                f"Stage '{stage_name}' expected dict payload, got {type(payload).__name__}"
            )

        missing = [key for key in self.required_keys if key not in payload]
        if missing:
            raise PayloadValidationError(
                f"Stage '{stage_name}' missing required keys: {', '.join(missing)}"
            )

        for key, expected_type in self.field_types.items():
            if key in payload and not isinstance(payload[key], expected_type):
                actual = type(payload[key]).__name__
                raise PayloadValidationError(
                    f"Stage '{stage_name}' key '{key}' expected {expected_type.__name__}, got {actual}"
                )

        if self.enforce_json_serializable:
            try:
                json.dumps(payload)
            except (TypeError, ValueError) as exc:
                raise PayloadValidationError(
                    f"Stage '{stage_name}' payload is not JSON-serializable: {exc}"
                ) from exc
