"""File upload stage for the embeddings pipeline.

This stage:
- Receives (chunk_ids, jsonl_content) items
- Uploads the JSONL content to OpenAI via `submit_input_file`
- Emits (file_id, chunk_ids) to the next stage
"""

from __future__ import annotations

from typing import Any

from embeddings.config import logger
from embeddings.openai_operations import submit_input_file
from embeddings.mongo_operations import store_file_metadata
from embeddings.stages.base_stage import PipelineStage


class FileUploadStage(PipelineStage):
    """Pipeline stage that uploads JSONL batch files to OpenAI.

    Input items are expected to be tuples of the form:
        (chunk_ids, jsonl_content)
    where:
        - chunk_ids: List[Tuple[int, int]]  # (qid, chunk_id)
        - jsonl_content: str                # JSONL request body for embeddings

    Output items (sent to `next_stage`) are tuples:
        (file_id, chunk_ids)
    """

    async def process_item(self, item: Any) -> Any:
        # Basic structural validation to fail fast if the item shape is wrong
        try:
            chunk_ids, jsonl_content = item
        except Exception as e:  # noqa: BLE001 - defensive guard
            logger.error(f"{self.name}: Invalid item shape for file upload: {item!r} ({e})")
            return None

        if not isinstance(jsonl_content, str):
            logger.error(f"{self.name}: jsonl_content must be a string, got {type(jsonl_content)}")
            return None

        try:
            logger.info(
                f"{self.name}: Uploading batch file for {len(chunk_ids)} chunks; "
                f"example ids: {chunk_ids[:3]}..."
            )
            file_id = await submit_input_file(jsonl_content)
            logger.info(
                f"{self.name}: Uploaded batch file {file_id} for {len(chunk_ids)} chunks"
            )
            # Store file metadata in MongoDB
            await store_file_metadata(file_id, chunk_ids, status="uploaded")
            # Pass (file_id, chunk_ids) to the next stage
            return file_id, chunk_ids
        except Exception as e:  # noqa: BLE001 - log and drop this item
            logger.error(
                f"{self.name}: Error uploading batch file for chunks {chunk_ids[:3]}... "
                f"({len(chunk_ids)} total): {e}"
            )
            return None
