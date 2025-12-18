"""Embedding upload stage for the embeddings pipeline.

This stage:
- Receives (batch_id, output_file_id) tuples from BatchPollingStage
- Downloads embeddings from OpenAI output file
- Uploads embeddings to MongoDB embeddings collection
- Marks batch as processed in MongoDB
"""

from __future__ import annotations

from typing import Any

from embeddings.config import logger
from embeddings.openai_operations import download_batch_results
from embeddings.mongo_operations import upload_embeddings_to_mongo, mark_batch_as_processed
from embeddings.stages.base_stage import PipelineStage


class EmbeddingUploadStage(PipelineStage):
    """Pipeline stage that downloads and uploads embeddings to MongoDB.

    Input items are expected to be tuples:
        (batch_id: str, output_file_id: str)  # From BatchPollingStage

    This is the final stage, so it does not forward items to any next stage.
    """

    async def process_item(self, item: Any) -> None:
        """Download embeddings from OpenAI and upload to MongoDB."""
        # Validate item structure
        if not isinstance(item, tuple) or len(item) != 2:
            logger.error(
                f"{self.name}: Invalid item type for embedding upload: "
                f"expected tuple (batch_id, output_file_id), got {type(item)}"
            )
            return None

        batch_id, output_file_id = item
        if not isinstance(batch_id, str) or not isinstance(output_file_id, str):
            logger.error(
                f"{self.name}: Invalid item structure: batch_id and output_file_id must be strings"
            )
            return None

        logger.info(
            f"{self.name}: Processing batch {batch_id} (output_file_id: {output_file_id})"
        )

        try:
            # 1. Download embeddings from OpenAI output file
            logger.info(f"{self.name}: Downloading embeddings for batch {batch_id}...")
            embeddings = await download_batch_results(batch_id, output_file_id)

            if not embeddings:
                logger.warning(
                    f"{self.name}: No embeddings downloaded for batch {batch_id}. "
                    "Skipping MongoDB upload."
                )
                return None

            logger.info(
                f"{self.name}: Downloaded {len(embeddings)} embeddings for batch {batch_id}"
            )

            # 2. Upload embeddings to MongoDB
            logger.info(
                f"{self.name}: Uploading {len(embeddings)} embeddings to MongoDB for batch {batch_id}..."
            )
            await upload_embeddings_to_mongo(embeddings)

            # 3. Mark batch as processed in MongoDB
            await mark_batch_as_processed(batch_id)

            logger.info(
                f"{self.name}: Successfully processed batch {batch_id} "
                f"({len(embeddings)} embeddings uploaded)"
            )

            # This is the final stage, so return None (no next stage)
            return None

        except Exception as e:
            logger.error(
                f"{self.name}: Error processing batch {batch_id}: {e}",
                exc_info=True,
            )
            # Don't trigger pipeline shutdown for individual batch failures
            # (we want to continue processing other batches)
            return None
