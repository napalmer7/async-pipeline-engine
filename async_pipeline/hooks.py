import inspect
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional


@dataclass(frozen=True)
class LifecycleEvent:
    """Event emitted during pipeline task and stage transitions."""

    event_type: str
    task_id: str
    stage_name: Optional[str] = None
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    details: Dict[str, Any] = field(default_factory=dict)


HookCallback = Callable[[LifecycleEvent], Any | Awaitable[Any]]


class EventBus:
    """Pub/sub event dispatcher supporting sync and async lifecycle subscribers."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[HookCallback]] = defaultdict(list)

    def subscribe(self, event_type: str, callback: HookCallback) -> None:
        """Subscribe a callback to a specific event type or '*' for all events."""
        self._subscribers[event_type].append(callback)

    async def emit(self, event: LifecycleEvent) -> None:
        """Dispatch an event to all matching subscribers."""
        targets = list(self._subscribers.get(event.event_type, [])) + list(
            self._subscribers.get("*", [])
        )
        for callback in targets:
            result = callback(event)
            if inspect.isawaitable(result):
                await result


class MetricsCollector:
    """In-memory telemetry collector tracking stage counts, errors, and latencies."""

    def __init__(self) -> None:
        self.task_submitted: int = 0
        self.task_completed: int = 0
        self.task_failed: int = 0
        self.stage_success_counts: Dict[str, int] = defaultdict(int)
        self.stage_failure_counts: Dict[str, int] = defaultdict(int)
        self.stage_durations_ms: Dict[str, List[float]] = defaultdict(list)

    def attach(self, bus: EventBus) -> None:
        """Register collector handlers onto an EventBus instance."""
        bus.subscribe("*", self.handle_event)

    def handle_event(self, event: LifecycleEvent) -> None:
        if event.event_type == "task_submitted":
            self.task_submitted += 1
        elif event.event_type == "task_completed":
            self.task_completed += 1
        elif event.event_type == "task_failed":
            self.task_failed += 1
        elif event.event_type == "stage_completed" and event.stage_name:
            self.stage_success_counts[event.stage_name] += 1
            duration = event.details.get("duration_ms")
            if isinstance(duration, (int, float)):
                self.stage_durations_ms[event.stage_name].append(float(duration))
        elif event.event_type == "stage_failed" and event.stage_name:
            self.stage_failure_counts[event.stage_name] += 1

    def average_duration_ms(self, stage_name: str) -> float:
        samples = self.stage_durations_ms.get(stage_name, [])
        if not samples:
            return 0.0
        return sum(samples) / len(samples)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "tasks": {
                "submitted": self.task_submitted,
                "completed": self.task_completed,
                "failed": self.task_failed,
            },
            "stages": {
                stage: {
                    "succeeded": self.stage_success_counts[stage],
                    "failed": self.stage_failure_counts[stage],
                    "avg_ms": round(self.average_duration_ms(stage), 3),
                }
                for stage in set(self.stage_success_counts) | set(self.stage_failure_counts)
            },
        }
