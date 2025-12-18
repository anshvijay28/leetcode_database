"""Batch creation stage for the embeddings pipeline.

This stage:
- Receives (file_id, chunk_ids) items from FilePollingStage
- Creates a batch job using submit_embedding_batch()
- Stores batch metadata in MongoDB
- Emits batch_id to the next stage
"""

from __future__ import annotations

from typing import Any

from embeddings.config import logger
from embeddings.openai_operations import submit_embedding_batch
from embeddings.mongo_operations import store_batch_metadata
from embeddings.stages.base_stage import PipelineStage


class BatchCreationStage(PipelineStage):
    """Pipeline stage that creates batch jobs from ready files.

    Input items are expected to be tuples of the form:
        (file_id, chunk_ids)
    where:
        - file_id: str                    # OpenAI file ID (file must be processed)
        - chunk_ids: List[Tuple[int, int]] # (qid, chunk_id) tuples

    Output items (sent to `next_stage`) are:
        batch_id: str  # OpenAI batch ID
    """

    async def process_item(self, item: Any) -> Any:
        """Create a batch job from a ready file."""
        # Validate item structure
        try:
            file_id, chunk_ids = item
        except Exception as e:  # noqa: BLE001
            logger.error(f"{self.name}: Invalid item shape for batch creation: {item!r} ({e})")
            return None

        if not isinstance(file_id, str):
            logger.error(f"{self.name}: file_id must be a string, got {type(file_id)}")
            return None

        try:
            logger.info(
                f"{self.name}: Creating batch for file {file_id} with {len(chunk_ids)} chunks"
            )
            # Create the batch job
            batch_id = await submit_embedding_batch(file_id)
            logger.info(
                f"{self.name}: Created batch {batch_id} for file {file_id} "
                f"({len(chunk_ids)} chunks)"
            )
            # Store batch metadata in MongoDB with initial status "validating"
            await store_batch_metadata(
                batch_id=batch_id,
                file_id=file_id,
                chunk_ids=chunk_ids,
                status="validating",
                retry_of=None,
            )
            # Forward batch_id to next stage
            return batch_id
        except Exception as e:  # noqa: BLE001
            logger.error(
                f"{self.name}: Error creating batch for file {file_id} "
                f"({len(chunk_ids)} chunks): {e}"
            )
            return None
