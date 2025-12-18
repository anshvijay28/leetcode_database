"""Main script for the embeddings pipeline.

This script orchestrates the complete embeddings pipeline:
FileUploadStage → FilePollingStage → BatchCreationStage → 
BatchPollingStage → EmbeddingUploadStage

Flow:
1. Fetch up to MAX_CHUNKS_PER_ITERATION chunks without batches
2. Convert them into JSONL batches
3. Enqueue each (chunk_ids, jsonl_content) into the pipeline
4. Pipeline processes: upload files → poll files → create batches → 
   poll batches → upload embeddings to MongoDB
5. Wait for the pipeline to drain, then fetch the next chunk batch
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from embeddings.config import (
    async_mongo_client,
    logger,
    EMBEDDING_BATCH_SIZE,
    MAX_CHUNKS_PER_ITERATION,
    MAX_CONCURRENT_FILE_UPLOADS,
    MAX_CONCURRENT_FILE_POLLING,
    MAX_CONCURRENT_BATCH_CREATIONS,
    MAX_CONCURRENT_BATCH_POLLING,
    MAX_CONCURRENT_EMBEDDING_UPLOADS,
)
from embeddings.mongo_operations import get_chunks_without_batches, get_incomplete_batch_ids
from embeddings.data_formatting import chunks_to_jsonl_content  # noqa: E402
from embeddings.pipeline import EmbeddingPipeline  # noqa: E402
from embeddings.stages.file_upload_stage import FileUploadStage  # noqa: E402
from embeddings.stages.file_polling_stage import FilePollingStage  # noqa: E402
from embeddings.stages.batch_creation_stage import BatchCreationStage  # noqa: E402
from embeddings.stages.batch_polling_stage import BatchPollingStage  # noqa: E402
from embeddings.stages.embedding_upload_stage import EmbeddingUploadStage  # noqa: E402


# Load environment variables
load_dotenv()


async def main() -> None:
    """Test harness for the embeddings pipeline.

    This sets up a pipeline with FileUploadStage, FilePollingStage,
    BatchCreationStage, and BatchPollingStage and repeatedly:
    - pulls a window of chunks without batches from MongoDB,
    - splits them into JSONL batches, and
    - enqueues those batches into the pipeline.
    """
    # Create and configure the pipeline
    pipeline = EmbeddingPipeline()

    # Add the file upload stage
    file_upload_stage = FileUploadStage(
        name="file-upload",
        max_concurrent=MAX_CONCURRENT_FILE_UPLOADS,
    )
    pipeline.add_stage(file_upload_stage)

    # Add the file polling stage (chains after file upload)
    file_polling_stage = FilePollingStage(
        name="file-polling",
        max_concurrent=MAX_CONCURRENT_FILE_POLLING,
    )
    pipeline.add_stage(file_polling_stage)

    # Add the batch creation stage (chains after file polling)
    batch_creation_stage = BatchCreationStage(
        name="batch-creation",
        max_concurrent=MAX_CONCURRENT_BATCH_CREATIONS,
    )
    pipeline.add_stage(batch_creation_stage)

    # Add the batch polling stage (chains after batch creation)
    batch_polling_stage = BatchPollingStage(
        name="batch-polling",
        max_concurrent=MAX_CONCURRENT_BATCH_POLLING,
    )
    pipeline.add_stage(batch_polling_stage)

    # Add the embedding upload stage (chains after batch polling)
    embedding_upload_stage = EmbeddingUploadStage(
        name="embedding-upload",
        max_concurrent=MAX_CONCURRENT_EMBEDDING_UPLOADS,
    )
    pipeline.add_stage(embedding_upload_stage)

    try:
        # Start the pipeline (starts all stage workers)
        logger.info("[Main] Starting pipeline...")
        await pipeline.start()

        # Resume incomplete batches from previous runs
        incomplete_batch_ids = await get_incomplete_batch_ids()
        if incomplete_batch_ids:
            logger.info(
                "[Main] Resuming %d incomplete batches from previous run",
                len(incomplete_batch_ids),
            )
            batch_polling_stage = pipeline.get_stage_by_name("batch-polling")
            if batch_polling_stage is None:
                logger.warning(
                    "[Main] Could not find batch-polling stage to resume batches"
                )
            else:
                for batch_id in incomplete_batch_ids:
                    await batch_polling_stage.enqueue(batch_id)
                logger.info(
                    "[Main] Enqueued %d incomplete batches to batch-polling stage",
                    len(incomplete_batch_ids),
                )
        else:
            logger.info("[Main] No incomplete batches to resume")

        total_batches_enqueued = 0

        while True:
            # 1. Fetch up to MAX_CHUNKS_PER_ITERATION chunks that do not yet
            #    belong to any embeddings batch.
            chunks_without_batches = await get_chunks_without_batches(
                limit=MAX_CHUNKS_PER_ITERATION
            )
            if not chunks_without_batches:
                logger.info("[Main] No more chunks without batches. Exiting loop.")
                break

            logger.info(
                "[Main] Fetched %d chunks without batches (limit=%d)",
                len(chunks_without_batches),
                MAX_CHUNKS_PER_ITERATION,
            )

            # 2. Turn chunks into JSONL batches of size EMBEDDING_BATCH_SIZE
            batch_contents = chunks_to_jsonl_content(
                chunks_without_batches,
                batch_size=int(EMBEDDING_BATCH_SIZE),
            )
            logger.info(
                "[Main] Created %d JSONL batches from %d chunks (batch_size=%d)",
                len(batch_contents),
                len(chunks_without_batches),
                int(EMBEDDING_BATCH_SIZE),
            )

            if not batch_contents:
                logger.info("[Main] No batch contents created; breaking loop.")
                break

            # 3. Enqueue each batch into the pipeline (goes to first stage)
            for chunk_ids, jsonl_content in batch_contents:
                await pipeline.enqueue((chunk_ids, jsonl_content))
                total_batches_enqueued += 1

            logger.info(
                "[Main] Enqueued %d batches into pipeline; waiting for queue to drain "
                "before fetching more chunks...",
                len(batch_contents),
            )

            # 4. Wait until all currently enqueued items are processed
            await pipeline.wait_for_completion()

            logger.info(
                "[Main] Iteration complete. Pipeline status: %s",
                pipeline.get_status(),
            )

        logger.info(
            "[Main] Pipeline complete. Total batches enqueued: %d",
            total_batches_enqueued,
        )

    except Exception as exc:  # noqa: BLE001
        logger.exception("[Main] Error in embeddings test harness: %s", exc)
        raise
    finally:
        # Shutdown the pipeline (stops all stage workers)
        logger.info("[Main] Shutting down pipeline...")
        await pipeline.shutdown()


        logger.info("[Main] Closing MongoDB connections...")
        await async_mongo_client.close()
        logger.info("[Main] MongoDB connections closed.")


if __name__ == "__main__":
    asyncio.run(main())
