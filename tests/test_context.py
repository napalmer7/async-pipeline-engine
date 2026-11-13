from datetime import datetime, timezone
import pytest
from async_pipeline.context import ExecutionContext, PayloadValidator
from async_pipeline.exceptions import PayloadValidationError


def test_execution_context_history_tracking():
    ctx = ExecutionContext(task_id="t-100", metadata={"env": "test"})
    now = datetime.now(timezone.utc)
    ctx.record_stage("extract", now, now, attempt=1, status="COMPLETED")

    assert len(ctx.history) == 1
    assert ctx.history[0].stage_name == "extract"
    assert ctx.history[0].status == "COMPLETED"


def test_payload_validator_checks_keys_and_types():
    validator = PayloadValidator(
        required_keys=["user_id", "count"],
        field_types={"user_id": str, "count": int},
    )
    validator.validate({"user_id": "u-1", "count": 5}, stage_name="validate_user")

    with pytest.raises(PayloadValidationError, match="missing required keys"):
        validator.validate({"user_id": "u-1"}, stage_name="validate_user")

    with pytest.raises(PayloadValidationError, match="expected int"):
        validator.validate({"user_id": "u-1", "count": "not-an-int"}, stage_name="validate_user")


def test_payload_validator_rejects_non_serializable():
    validator = PayloadValidator(enforce_json_serializable=True)
    with pytest.raises(PayloadValidationError, match="not JSON-serializable"):
        validator.validate({"bad": {1, 2, 3}}, stage_name="serialize_check")
