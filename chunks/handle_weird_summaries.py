"""
Script to chunk and upload summaries for specific qids.

This script is designed for handling a small number of summaries (e.g., 5)
that need to be processed manually. Uses process_summaries_batch with
max_concurrent=1 for sequential processing.
"""

import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from chunks.config import async_mongo_client, logger, EXCLUDED_PROBLEMS
from chunks.batch_processor import process_summaries_batch


async def main():
    """
    Process specific qids using process_summaries_batch.
    """
    # Example usage: process specific qids
    # Modify this list with the qids you want to process
    qids_to_process = []  # Add your qids here
    
    if not qids_to_process:
        qids_to_process = EXCLUDED_PROBLEMS
    
    try:
        logger.info(f"Processing {len(qids_to_process)} qids: {qids_to_process}")
        
        # Use process_summaries_batch with max_concurrent=1 for sequential processing
        await process_summaries_batch(
            qids=qids_to_process,
            batch_size=len(qids_to_process),  # Process all in one batch
            max_concurrent=1  # Sequential processing (one at a time)
        )
        
        logger.info("Processing completed successfully!")
    finally:
        # Close MongoDB connection
        await async_mongo_client.close()
        logger.info("MongoDB connection closed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

