import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from .context import ExecutionContext
from .exceptions import PipelineTimeoutError, StageExecutionError
from .hooks import EventBus, LifecycleEvent
from .persistence import StateJournal
from .policies import TimeoutPolicy, execute_with_policies
from .registry import StageRegistry


@dataclass
class PipelineTask:
    task_id: str
    stages: List[str]
    initial_payload: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0  # Priority weighting not yet implemented in queue dispatch
    metadata: Dict[str, Any] = field(default_factory=dict)


class PipelineRunner:
    """Core async worker engine executing multi-stage pipeline workflows."""

    def __init__(
        self,
        registry: StageRegistry,
        journal: Optional[StateJournal] = None,
        event_bus: Optional[EventBus] = None,
        timeout_policy: Optional[TimeoutPolicy] = None,
        max_concurrency: int = 4,
    ) -> None:
        self.registry = registry
        self.journal = journal
        self.event_bus = event_bus or EventBus()
        self.timeout_policy = timeout_policy
        self.queue: asyncio.Queue[PipelineTask] = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._running = False
        self._workers: List[asyncio.Task[Any]] = []

    async def submit(self, task: PipelineTask) -> None:
        """Enqueue a new pipeline task."""
        await self.queue.put(task)
        await self.event_bus.emit(
            LifecycleEvent(
                event_type="task_submitted",
                task_id=task.task_id,
                details={"priority": task.priority, "stages": list(task.stages)},
            )
        )

    async def execute_task(self, task: PipelineTask) -> Dict[str, Any]:
        """Execute all stages in sequence for a single task."""
        pipeline_timeout = (
            self.timeout_policy.pipeline_timeout_seconds if self.timeout_policy else None
        )

        async def _run_all_stages() -> Dict[str, Any]:
            async with self.semaphore:
                ctx = ExecutionContext(task_id=task.task_id, metadata=dict(task.metadata))
                current_payload = dict(task.initial_payload)

                for stage_name in task.stages:
                    stage_def = self.registry.get_definition(stage_name)
                    if stage_def.input_validator:
                        stage_def.input_validator.validate(current_payload, stage_name=stage_name)

                    await self.event_bus.emit(
                        LifecycleEvent(
                            event_type="stage_started",
                            task_id=task.task_id,
                            stage_name=stage_name,
                        )
                    )

                    started_dt = datetime.now(timezone.utc)
                    perf_start = time.perf_counter()
                    attempts = 1

                    try:
                        result, attempts = await execute_with_policies(
                            coro_factory=lambda h=stage_def.handler, p=current_payload: h(p),
                            stage_name=stage_name,
                            retry_policy=stage_def.retry_policy,
                            timeout_policy=self.timeout_policy,
                        )
                        duration_ms = (time.perf_counter() - perf_start) * 1000.0
                        finished_dt = datetime.now(timezone.utc)

                        current_payload.update(result)
                        ctx.record_stage(stage_name, started_dt, finished_dt, attempts, "COMPLETED")

                        if self.journal:
                            self.journal.save_checkpoint(
                                task_id=task.task_id,
                                stage_name=stage_name,
                                output_state=result,
                                status="COMPLETED",
                            )

                        await self.event_bus.emit(
                            LifecycleEvent(
                                event_type="stage_completed",
                                task_id=task.task_id,
                                stage_name=stage_name,
                                details={"attempts": attempts, "duration_ms": duration_ms},
                            )
                        )
                    except Exception as exc:
                        finished_dt = datetime.now(timezone.utc)
                        ctx.record_stage(stage_name, started_dt, finished_dt, attempts, "FAILED")

                        if self.journal:
                            self.journal.save_checkpoint(
                                task_id=task.task_id,
                                stage_name=stage_name,
                                output_state={"error": str(exc)},
                                status="FAILED",
                            )

                        await self.event_bus.emit(
                            LifecycleEvent(
                                event_type="stage_failed",
                                task_id=task.task_id,
                                stage_name=stage_name,
                                details={"error": str(exc)},
                            )
                        )
                        await self.event_bus.emit(
                            LifecycleEvent(
                                event_type="task_failed",
                                task_id=task.task_id,
                                stage_name=stage_name,
                                details={"error": str(exc)},
                            )
                        )
                        raise StageExecutionError(stage_name, exc, attempts=attempts) from exc

                await self.event_bus.emit(
                    LifecycleEvent(
                        event_type="task_completed",
                        task_id=task.task_id,
                        details={"stages_completed": len(ctx.history)},
                    )
                )
                return current_payload

        if pipeline_timeout is not None:
            try:
                return await asyncio.wait_for(_run_all_stages(), timeout=pipeline_timeout)
            except asyncio.TimeoutError as exc:
                raise PipelineTimeoutError(
                    f"Task '{task.task_id}' exceeded pipeline timeout of {pipeline_timeout}s"
                ) from exc

        return await _run_all_stages()

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                task = await self.queue.get()
                try:
                    await self.execute_task(task)
                except Exception:
                    pass  # Failures are journaled and emitted via EventBus
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break

    async def start(self, num_workers: int = 2) -> None:
        """Start background queue workers."""
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_loop())
            for _ in range(num_workers)
        ]

    async def stop(self) -> None:
        """Stop all background workers gracefully."""
        self._running = False
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
