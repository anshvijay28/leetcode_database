"""Base pipeline stage abstraction for the embeddings pipeline.

Each stage:
- Owns an asyncio.Queue for its input
- Runs a background worker loop pulling items from the queue
- Applies a stage-specific `process_item` implementation
- Optionally forwards results to the next stage

Concrete stages should subclass `PipelineStage` and implement `process_item`.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from embeddings.pipeline import EmbeddingPipeline


class PipelineStage(ABC):
    """Abstract base class for a single stage in the pipeline.

    Responsibilities:
    - Manage an input queue of items to process
    - Run multiple worker tasks that continuously process items
    - Achieve concurrency by having several workers share one queue
    - Optionally forward processed results to the next stage
    """

    def __init__(
        self,
        name: str,
        max_concurrent: int,
        next_stage: Optional["PipelineStage"] = None,
    ) -> None:
        self.name = name
        self.input_queue: "asyncio.Queue[Any]" = asyncio.Queue()
        self.max_concurrent = max_concurrent
        self.next_stage = next_stage

        # Multiple worker tasks share the same input_queue to process items concurrently
        self._worker_tasks: list[asyncio.Task] = []
        self._running: bool = False
        self._processed_count: int = 0
        self.pipeline: Optional["EmbeddingPipeline"] = None

    @abstractmethod
    async def process_item(self, item: Any) -> Any:
        """Process a single item.

        Concrete subclasses must implement this and may return either:
        - A single result to be enqueued into the next stage
        - None if there is nothing to forward
        """

    async def _worker_loop(self) -> None:
        """Background loop that pulls items from the queue and processes them.

        Multiple instances of this coroutine may run in parallel (one per worker),
        all sharing the same input queue.
        """
        try:
            while self._running:
                item = await self.input_queue.get()
                try:
                    result = await self.process_item(item)
                    self._processed_count += 1

                    # Forward to next stage if there is one and we have a result
                    if self.next_stage is not None and result is not None:
                        await self.next_stage.enqueue(result)
                finally:
                    self.input_queue.task_done()
        except asyncio.CancelledError:
            # Graceful shutdown: allow the task to be cancelled cleanly
            pass

    async def start_workers(self) -> None:
        """Start background worker tasks if not already running.

        Spawns `max_concurrent` worker tasks, each consuming from the same queue.
        """
        if any(not t.done() for t in self._worker_tasks):
            return

        self._running = True
        self._worker_tasks = [
            asyncio.create_task(self._worker_loop(), name=f"{self.name}-worker-{i}")
            for i in range(self.max_concurrent)
        ]

    async def stop_workers(self) -> None:
        """Signal all workers to stop and wait for them to finish."""
        self._running = False
        for task in self._worker_tasks:
            if not task.done():
                task.cancel()
        for task in self._worker_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._worker_tasks.clear()

    def set_pipeline(self, pipeline: "EmbeddingPipeline") -> None:
        """Set the pipeline reference so this stage can trigger shutdown on failure.
        
        Args:
            pipeline: The EmbeddingPipeline instance
        """
        self.pipeline = pipeline

    async def enqueue(self, item: Any) -> None:
        """Add a new item to this stage's input queue."""
        await self.input_queue.put(item)

    def get_status(self) -> dict:
        """Return a lightweight snapshot of this stage's status."""
        return {
            "name": self.name,
            "queue_size": self.input_queue.qsize(),
            "num_workers": len(self._worker_tasks),
            "max_concurrent": self.max_concurrent,
            "processed_count": self._processed_count,
            "running": self._running,
        }
