"""
Main script for batch chunk processing.

This script orchestrates the complete batch processing pipeline:
1. Get all qids to process (excluding EXCLUDED_PROBLEMS)
2. Process summaries in batches (fetch, chunk, insert)
3. Create index on chunks collection
4. Close MongoDB connections
"""

import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from chunks.config import async_mongo_client, logger, EXCLUDED_PROBLEMS, chunks_collection
from chunks.database import get_qids_to_process
from chunks.batch_processor import process_summaries_batch

async def create_index() -> None:
    """Create compound unique index on (qid, chunk_id) for efficient upserts."""
    
    try:
        # Create compound unique index
        await chunks_collection.create_index(
            [("qid", 1), ("chunk_id", 1)],
            unique=True,
            name="qid_chunk_id_unique"
        )
        logger.info("Created compound unique index on (qid, chunk_id)")
    except Exception as e:
        # Index might already exist, which is fine
        if "already exists" in str(e).lower() or "duplicate key" in str(e).lower():
            logger.info("Index on (qid, chunk_id) already exists")
        else:
            logger.warning(f"Could not create index (may already exist): {e}")


async def main():
    """
    Main function to orchestrate batch chunk processing.
    """
    try:
        # Step 1: Create index (if it doesn't exist)
        logger.info("Ensuring index exists on chunks collection...")
        await create_index()
        
        # Step 2: Get all qids to process
        logger.info("Fetching qids to process...")
        qids = await get_qids_to_process(EXCLUDED_PROBLEMS)
        logger.info(f"Found {len(qids)} qids to process (excluding {len(EXCLUDED_PROBLEMS)} excluded problems)")
        
        if not qids:
            logger.warning("No qids to process. Exiting.")
            return
        
        # Step 3: Process summaries in batches
        # Configuration:
        # - batch_size: Number of qids to process per batch (100 = fetch 100 summaries at once)
        # - max_concurrent: Number of concurrent chunking operations per batch (10 = chunk 10 summaries simultaneously)
        await process_summaries_batch(
            qids=qids,
            batch_size=200,      # Process 200 qids per batch
            max_concurrent=16     # 2x CPU cores
        )
        
        logger.info("Batch processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        # Close MongoDB connections
        logger.info("Closing MongoDB connections...")
        await async_mongo_client.close()
        logger.info("MongoDB connections closed")


if __name__ == "__main__":
    asyncio.run(main())
