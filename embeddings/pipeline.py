"""Pipeline orchestration class for the embeddings pipeline.

This class manages multiple PipelineStage instances, chains them together,
and provides a unified interface for starting, enqueuing items, monitoring,
and shutting down the entire pipeline.
"""

from __future__ import annotations

from typing import Any, List, Optional

from embeddings.config import logger
from embeddings.stages.base_stage import PipelineStage


class EmbeddingPipeline:
    """Orchestrates multiple pipeline stages into a cohesive processing pipeline.

    Responsibilities:
    - Chain stages together (output of stage N becomes input of stage N+1)
    - Start/stop all stage workers
    - Enqueue items to the first stage
    - Monitor pipeline status
    - Wait for pipeline completion
    """

    def __init__(self) -> None:
        """Initialize an empty pipeline."""
        self.stages: List[PipelineStage] = []
        self._started: bool = False
        self._shutdown_reason: Optional[str] = None

    def add_stage(self, stage: PipelineStage) -> None:
        """Add a stage to the pipeline.

        Stages are added in order, and each stage's output is automatically
        forwarded to the next stage (if one exists).

        Args:
            stage: The PipelineStage instance to add
        """
        if self._started:
            raise RuntimeError(
                "Cannot add stages after pipeline has been started. "
                "Call shutdown() first, then add stages."
            )

        # If this isn't the first stage, link it to the previous one
        if self.stages:
            self.stages[-1].next_stage = stage

        self.stages.append(stage)
        # Give the stage a reference to the pipeline so it can trigger shutdown
        stage.set_pipeline(self)
        logger.info(f"[Pipeline] Added stage: {stage.name}")

    async def start(self) -> None:
        """Start all stage workers in the pipeline."""
        if not self.stages:
            raise RuntimeError("Cannot start pipeline: no stages added")

        if self._started:
            logger.warning("[Pipeline] Pipeline already started, ignoring start() call")
            return

        logger.info(f"[Pipeline] Starting {len(self.stages)} stages...")
        for stage in self.stages:
            await stage.start_workers()
        self._started = True
        logger.info("[Pipeline] All stages started")

    async def enqueue(self, item: Any) -> None:
        """Enqueue an item into the first stage of the pipeline.

        Args:
            item: The item to process (will be sent to the first stage)
        """
        if not self.stages:
            raise RuntimeError("Cannot enqueue: pipeline has no stages")
        if not self._started:
            raise RuntimeError("Cannot enqueue: pipeline not started. Call start() first.")

        await self.stages[0].enqueue(item)

    def get_stage_by_name(self, name: str) -> Optional[PipelineStage]:
        """Get a stage by its name.

        Args:
            name: The name of the stage to retrieve

        Returns:
            The PipelineStage with the matching name, or None if not found
        """
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None

    async def wait_for_completion(self) -> None:
        """Wait until all stages' queues are empty and all items are processed.

        This waits for:
        - All items in all stage queues to be processed
        - All in-flight items to complete
        """
        if not self._started:
            return

        logger.info("[Pipeline] Waiting for all stages to complete...")
        for stage in self.stages:
            await stage.input_queue.join()
        logger.info("[Pipeline] All stages completed")

    def get_status(self) -> dict:
        """Get a status snapshot of all stages in the pipeline.

        Returns:
            Dictionary with pipeline status, including per-stage details
        """
        return {
            "started": self._started,
            "num_stages": len(self.stages),
            "stages": [stage.get_status() for stage in self.stages],
        }

    async def trigger_shutdown(self, reason: str) -> None:
        """Trigger pipeline shutdown with a reason.
        
        Args:
            reason: Reason for shutdown (e.g., "File upload failed")
        """
        if not self._started:
            return
        
        self._shutdown_reason = reason
        logger.error(f"[Pipeline] Shutting down pipeline: {reason}")
        await self.shutdown()

    async def shutdown(self) -> None:
        """Stop all stage workers and clean up resources."""
        if not self._started:
            return

        shutdown_msg = f"[Pipeline] Shutting down all stages"
        if self._shutdown_reason:
            shutdown_msg += f" (reason: {self._shutdown_reason})"
        logger.info(shutdown_msg)
        
        # Stop stages in reverse order (last stage first)
        for stage in reversed(self.stages):
            await stage.stop_workers()
        self._started = False
        logger.info("[Pipeline] Pipeline shutdown complete")
