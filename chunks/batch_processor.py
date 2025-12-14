"""
Batch processing module for chunks.

This module handles the orchestration of batch processing operations:
fetching summaries, chunking them, and inserting chunks to MongoDB.
"""

import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from tqdm.asyncio import tqdm as async_tqdm

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from chunks.config import logger
from chunks.text_processing import chunk_summary
from chunks.database import batch_get_summaries, batch_insert_chunks


async def process_summaries_batch(qids: list[int], batch_size: int = 100, max_concurrent: int = 10) -> None:
    """
    Process summaries in batches: fetch, chunk, and insert chunks to MongoDB.
    
    For each batch:
    1. Fetch all summaries in a single MongoDB query
    2. Process summaries concurrently (CPU-bound chunking)
    3. Batch insert all chunks to MongoDB
    
    Args:
        qids: List of question IDs to process
        batch_size: Number of qids to process per batch (default: 100)
        max_concurrent: Maximum concurrent chunking operations per batch (default: 10)
    """
    if not qids:
        logger.info("No qids to process")
        return
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_single_qid(qid: int, summary_text: str) -> list[dict]:
        """Process a single qid: chunk the summary and return chunks."""
        async with semaphore:
            try:
                # Run CPU-bound chunking in thread pool to avoid blocking
                chunks = await asyncio.to_thread(chunk_summary, qid, summary_text)
                return chunks
            except Exception as e:
                logger.error(f"Error chunking summary for qid {qid}: {e}")
                return []
    
    # Process qids in batches
    total_batches = (len(qids) + batch_size - 1) // batch_size
    logger.info(f"Processing {len(qids)} qids in {total_batches} batches (batch_size={batch_size})")
    
    with async_tqdm(total=len(qids), desc="Processing summaries") as pbar:
        for batch_idx in range(0, len(qids), batch_size):
            batch_qids = qids[batch_idx:batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1
            
            try:
                # Step 1: Fetch all summaries in batch (single MongoDB query)
                logger.info(f"Batch {batch_num}/{total_batches}: Fetching {len(batch_qids)} summaries...")
                summaries = await batch_get_summaries(batch_qids)
                
                if not summaries:
                    logger.warning(f"Batch {batch_num}: No summaries found for {len(batch_qids)} qids")
                    pbar.update(len(batch_qids))
                    continue
                
                # Step 2: Process summaries concurrently (CPU-bound chunking)
                logger.info(f"Batch {batch_num}: Chunking {len(summaries)} summaries...")
                tasks = [
                    process_single_qid(qid, summary_text)
                    for qid, summary_text in summaries.items()
                ]
                
                # Collect all chunks from concurrent processing
                chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Flatten chunks and filter out errors
                all_chunks = []
                for result in chunk_results:
                    if isinstance(result, Exception):
                        logger.error(f"Error in chunking task: {result}")
                    elif isinstance(result, list):
                        all_chunks.extend(result)
                
                # Step 3: Batch insert all chunks to MongoDB
                if all_chunks:
                    logger.info(f"Batch {batch_num}: Inserting {len(all_chunks)} chunks...")
                    await batch_insert_chunks(all_chunks)
                    logger.info(f"Batch {batch_num}: Completed - {len(summaries)} summaries, {len(all_chunks)} chunks")
                else:
                    logger.warning(f"Batch {batch_num}: No chunks generated")
                
                # Update progress bar
                pbar.update(len(batch_qids))
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                pbar.update(len(batch_qids))
                continue
    
    logger.info(f"Completed processing {len(qids)} qids")
