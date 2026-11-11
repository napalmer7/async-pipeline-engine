import pytest
from async_pipeline.exceptions import StageNotFoundError
from async_pipeline.registry import StageRegistry


@pytest.mark.asyncio
async def test_register_and_get_handler():
    registry = StageRegistry()

    @registry.register("normalize")
    async def normalize_data(payload):
        return {"normalized": True}

    assert registry.has_stage("normalize")
    handler = registry.get("normalize")
    result = await handler({})
    assert result == {"normalized": True}


def test_missing_handler_raises():
    registry = StageRegistry()
    with pytest.raises(StageNotFoundError):
        registry.get("nonexistent")
