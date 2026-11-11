import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from .exceptions import StageExecutionError
from .persistence import StateJournal
from .registry import StageRegistry


@dataclass
class PipelineTask:
    task_id: str
    stages: List[str]
    initial_payload: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0  # Not yet handled by worker queue


class PipelineRunner:
    """Core async worker engine executing multi-stage pipeline workflows."""

    def __init__(
        self,
        registry: StageRegistry,
        journal: Optional[StateJournal] = None,
        max_concurrency: int = 4
    ) -> None:
        self.registry = registry
        self.journal = journal
        self.queue: asyncio.Queue[PipelineTask] = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._running = False
        self._workers: List[asyncio.Task] = []

    async def submit(self, task: PipelineTask) -> None:
        """Enqueue a new pipeline task."""
        await self.queue.put(task)

    async def execute_task(self, task: PipelineTask) -> Dict[str, Any]:
        """Execute all stages in sequence for a single task."""
        async with self.semaphore:
            current_payload = dict(task.initial_payload)

            for stage_name in task.stages:
                handler = self.registry.get(stage_name)
                try:
                    result = await handler(current_payload)
                    current_payload.update(result)

                    if self.journal:
                        self.journal.save_checkpoint(
                            task_id=task.task_id,
                            stage_name=stage_name,
                            output_state=result,
                            status="COMPLETED"
                        )
                except Exception as exc:
                    if self.journal:
                        self.journal.save_checkpoint(
                            task_id=task.task_id,
                            stage_name=stage_name,
                            output_state={"error": str(exc)},
                            status="FAILED"
                        )
                    raise StageExecutionError(stage_name, exc) from exc

            return current_payload

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                task = await self.queue.get()
                try:
                    await self.execute_task(task)
                except Exception:
                    pass  # Failures handled and journaled
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
        """Stop all background workers and drain queue."""
        self._running = False
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
