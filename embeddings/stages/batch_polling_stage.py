"""Batch polling stage for the embeddings pipeline.

This stage:
- Receives batch_id items from BatchCreationStage
- Polls batch status every EMBEDDING_POLL_INTERVAL seconds until terminal state
- If "failed": Updates MongoDB and triggers pipeline shutdown
- If "completed": Updates MongoDB and forwards (batch_id, output_file_id) to next stage
"""

from __future__ import annotations

import asyncio
from typing import Any

from embeddings.config import logger, EMBEDDING_POLL_INTERVAL
from embeddings.openai_operations import poll_batch_status
from embeddings.mongo_operations import update_batch_metadata_status
from embeddings.stages.base_stage import PipelineStage


class BatchPollingStage(PipelineStage):
    """Pipeline stage that polls batch status until completion or failure.

    Input items are expected to be:
        batch_id: str  # OpenAI batch ID

    Output items (sent to `next_stage`) are tuples:
        (batch_id, output_file_id)  # Only forwarded if batch status is "completed"
    """

    async def process_item(self, item: Any) -> Any:
        """Poll batch status until it reaches a terminal state (completed or failed)."""
        # Validate item structure
        if not isinstance(item, str):
            logger.error(
                f"{self.name}: Invalid item type for batch polling: expected str, got {type(item)}"
            )
            return None

        batch_id = item
        terminal_states = {"completed", "failed", "expired", "cancelled", "error"}
        poll_interval = int(EMBEDDING_POLL_INTERVAL)

        logger.info(f"{self.name}: Starting to poll batch {batch_id}")

        # Poll batch status until it reaches a terminal state
        while True:
            status, output_file_id = await poll_batch_status(batch_id)

            # Update MongoDB with current status (poll_batch_status already updates, but
            # we'll also call update_batch_metadata_status for consistency)
            await update_batch_metadata_status(batch_id, status)

            # Check for failure state
            if status == "failed":
                logger.error(
                    f"{self.name}: Batch {batch_id} failed during processing"
                )
                # Trigger pipeline shutdown
                if self.pipeline is not None:
                    await self.pipeline.trigger_shutdown(
                        reason=f"Batch processing failed: {batch_id}"
                    )
                # Don't forward to next stage
                return None

            # Check if batch is completed
            if status == "completed":
                if not output_file_id:
                    logger.warning(
                        f"{self.name}: Batch {batch_id} completed but no output_file_id found"
                    )
                    return None

                logger.info(
                    f"{self.name}: Batch {batch_id} completed successfully. "
                    f"Output file: {output_file_id}"
                )
                # Forward to next stage
                return batch_id, output_file_id

            # Check if batch reached other terminal states (expired, cancelled, error)
            if status in terminal_states:
                logger.warning(
                    f"{self.name}: Batch {batch_id} reached terminal state: {status}"
                )
                # Trigger pipeline shutdown for unexpected terminal states
                if self.pipeline is not None:
                    await self.pipeline.trigger_shutdown(
                        reason=f"Batch reached terminal state {status}: {batch_id}"
                    )
                return None

            # Batch is still processing, wait before next poll
            logger.debug(
                f"{self.name}: Batch {batch_id} status={status}; "
                f"sleeping {poll_interval}s before next poll"
            )
            await asyncio.sleep(poll_interval)
