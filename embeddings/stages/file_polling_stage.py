"""File polling stage for the embeddings pipeline.

This stage:
- Receives (file_id, chunk_ids) items from FileUploadStage
- Polls file status every 3 seconds until it reaches "processed" or "failed"
- If "failed": Updates MongoDB and triggers pipeline shutdown
- If "processed": Updates MongoDB and forwards (file_id, chunk_ids) to next stage
"""

from __future__ import annotations

import asyncio
from typing import Any

from embeddings.config import logger
from embeddings.openai_operations import poll_file_status
from embeddings.mongo_operations import update_file_metadata_status
from embeddings.stages.base_stage import PipelineStage


class FilePollingStage(PipelineStage):
    """Pipeline stage that polls file status until ready or failed.

    Input items are expected to be tuples of the form:
        (file_id, chunk_ids)
    where:
        - file_id: str                    # OpenAI file ID
        - chunk_ids: List[Tuple[int, int]] # (qid, chunk_id) tuples

    Output items (sent to `next_stage`) are tuples:
        (file_id, chunk_ids)  # Only forwarded if file status is "processed"
    """

    FILE_POLL_INTERVAL = 3  # Poll every 3 seconds

    async def process_item(self, item: Any) -> Any:
        """Poll file status until it reaches a terminal state (processed or failed)."""
        # Validate item structure
        try:
            file_id, chunk_ids = item
        except Exception as e:  # noqa: BLE001
            logger.error(f"{self.name}: Invalid item shape for file polling: {item!r} ({e})")
            return None

        if not isinstance(file_id, str):
            logger.error(f"{self.name}: file_id must be a string, got {type(file_id)}")
            return None

        logger.info(
            f"{self.name}: Starting to poll file {file_id} for {len(chunk_ids)} chunks"
        )

        # Poll file status until it reaches a terminal state
        while True:
            status, is_ready = await poll_file_status(file_id)

            await update_file_metadata_status(file_id, status=status)

            # Check for failure state
            if status == "failed":
                logger.error(
                    f"{self.name}: File {file_id} failed during processing. "
                    f"Affected chunks: {len(chunk_ids)}"
                )

                # Trigger pipeline shutdown
                if self.pipeline is not None:
                    await self.pipeline.trigger_shutdown(
                        reason=f"File upload failed: {file_id}"
                    )
                # Don't forward to next stage
                return None

            # Check if file is ready (processed)
            if is_ready:
                logger.info(
                    f"{self.name}: File {file_id} is ready (status: {status}) "
                    f"for {len(chunk_ids)} chunks"
                )
                # Forward to next stage
                return file_id, chunk_ids

            # File is still processing, wait before next poll
            logger.debug(
                f"{self.name}: File {file_id} status={status}; "
                f"sleeping {self.FILE_POLL_INTERVAL}s before next poll"
            )
            await asyncio.sleep(self.FILE_POLL_INTERVAL)
