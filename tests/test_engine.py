import pytest
from async_pipeline.context import PayloadValidator
from async_pipeline.engine import PipelineRunner, PipelineTask
from async_pipeline.exceptions import StageExecutionError
from async_pipeline.hooks import EventBus, MetricsCollector
from async_pipeline.policies import RetryPolicy
from async_pipeline.registry import StageRegistry


@pytest.mark.asyncio
async def test_pipeline_sequential_execution():
    registry = StageRegistry()

    @registry.register("step_one")
    async def step_one(payload):
        return {"val": payload.get("val", 0) + 1}

    @registry.register("step_two")
    async def step_two(payload):
        return {"val": payload["val"] * 2}

    runner = PipelineRunner(registry)
    task = PipelineTask(
        task_id="task-1",
        stages=["step_one", "step_two"],
        initial_payload={"val": 5},
    )

    result = await runner.execute_task(task)
    assert result["val"] == 12


@pytest.mark.asyncio
async def test_pipeline_failure_propagation_and_metrics():
    registry = StageRegistry()
    bus = EventBus()
    metrics = MetricsCollector()
    metrics.attach(bus)

    @registry.register("faulty_stage")
    async def faulty_stage(payload):
        raise ValueError("Invalid input")

    runner = PipelineRunner(registry, event_bus=bus)
    task = PipelineTask(task_id="fail-task", stages=["faulty_stage"])

    with pytest.raises(StageExecutionError) as exc_info:
        await runner.execute_task(task)

    assert exc_info.value.stage_name == "faulty_stage"
    assert metrics.task_failed == 1


@pytest.mark.asyncio
async def test_pipeline_stage_retry_and_validation():
    registry = StageRegistry()
    attempts = 0

    @registry.register(
        "unstable_transform",
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.005),
        input_validator=PayloadValidator(required_keys=["seed"]),
    )
    async def unstable_transform(payload):
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise ConnectionError("Transient glitch")
        return {"computed": payload["seed"] * 10}

    runner = PipelineRunner(registry)
    task = PipelineTask(
        task_id="retry-task",
        stages=["unstable_transform"],
        initial_payload={"seed": 4},
    )

    result = await runner.execute_task(task)
    assert result["computed"] == 40
    assert attempts == 2
