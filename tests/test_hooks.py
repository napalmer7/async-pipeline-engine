import pytest
from async_pipeline.hooks import EventBus, LifecycleEvent, MetricsCollector


@pytest.mark.asyncio
async def test_event_bus_and_metrics_collector():
    bus = EventBus()
    metrics = MetricsCollector()
    metrics.attach(bus)

    seen_events = []
    bus.subscribe("stage_completed", lambda ev: seen_events.append(ev.stage_name))

    await bus.emit(LifecycleEvent(event_type="task_submitted", task_id="t-1"))
    await bus.emit(
        LifecycleEvent(
            event_type="stage_completed",
            task_id="t-1",
            stage_name="parse",
            details={"duration_ms": 12.5},
        )
    )
    await bus.emit(LifecycleEvent(event_type="task_completed", task_id="t-1"))

    assert seen_events == ["parse"]
    snap = metrics.snapshot()
    assert snap["tasks"]["submitted"] == 1
    assert snap["tasks"]["completed"] == 1
    assert snap["stages"]["parse"]["succeeded"] == 1
    assert snap["stages"]["parse"]["avg_ms"] == 12.5
