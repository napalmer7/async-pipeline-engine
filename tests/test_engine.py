import pytest
from async_pipeline.engine import PipelineRunner, PipelineTask
from async_pipeline.exceptions import StageExecutionError
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
        initial_payload={"val": 5}
    )

    result = await runner.execute_task(task)
    assert result["val"] == 12


@pytest.mark.asyncio
async def test_pipeline_failure_propagation():
    registry = StageRegistry()

    @registry.register("faulty_stage")
    async def faulty_stage(payload):
        raise ValueError("Invalid input")

    runner = PipelineRunner(registry)
    task = PipelineTask(
        task_id="fail-task",
        stages=["faulty_stage"]
    )

    with pytest.raises(StageExecutionError) as exc_info:
        await runner.execute_task(task)

    assert exc_info.value.stage_name == "faulty_stage"
