import asyncio
import pytest
from async_pipeline.exceptions import PipelineTimeoutError
from async_pipeline.policies import RetryPolicy, TimeoutPolicy, execute_with_policies


def test_retry_policy_delay_calculation():
    policy = RetryPolicy(max_attempts=4, base_delay=0.1, max_delay=0.35, backoff_multiplier=2.0)
    assert policy.compute_delay(1) == pytest.approx(0.1)
    assert policy.compute_delay(2) == pytest.approx(0.2)
    assert policy.compute_delay(3) == pytest.approx(0.35)


@pytest.mark.asyncio
async def test_execute_with_retry_succeeds_after_transient_error():
    calls = 0

    async def flaky_op():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("Temporary failure")
        return "ok"

    policy = RetryPolicy(max_attempts=3, base_delay=0.005)
    result, attempts = await execute_with_policies(flaky_op, "flaky_stage", retry_policy=policy)

    assert result == "ok"
    assert attempts == 3


@pytest.mark.asyncio
async def test_execute_with_stage_timeout():
    async def slow_op():
        await asyncio.sleep(0.2)
        return "late"

    timeout = TimeoutPolicy(stage_timeout_seconds=0.02)
    with pytest.raises(PipelineTimeoutError):
        await execute_with_policies(slow_op, "slow_stage", timeout_policy=timeout)
