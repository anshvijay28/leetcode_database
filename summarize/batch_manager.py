"""
Batch manager module for batch metadata management and orchestration.

This module handles:
- Storing batch metadata in MongoDB
- Resuming existing batches
- Submitting batches (orchestrates upload + create + store)
- Processing batch results (orchestrates download + upload summaries)
"""

import asyncio
from datetime import datetime
from typing import List, Dict, Tuple

from tqdm.asyncio import tqdm as async_tqdm

from summarize.config import (
    logger,
    batch_metadata_collection,
    summary_collection,
    MAX_CONCURRENT_BATCH_SUBMISSIONS,
    MAX_CONCURRENT_BATCH_PROCESSING,
)
from summarize.batch_api import upload_batch_file, create_batch, download_batch_results
from summarize.summary_storage import batch_upload_summaries


async def store_batch_metadata(batch_id: str, file_id: str, qids: List[int], status: str = "validating", retry_of: str = None):
    """
    Store batch metadata in MongoDB.
    
    Args:
        batch_id: OpenAI batch ID
        file_id: OpenAI file ID
        qids: List of question IDs in this batch
        status: Current batch status
        retry_of: Optional batch_id of the failed batch this retry replaces
    """
    metadata = {
        "batch_id": batch_id,
        "openai_file_id": file_id,
        "status": status,
        "qids": qids,
        "created_at": datetime.utcnow(),
        "processed": False
    }
    
    # Add retry tracking if this is a retry
    if retry_of:
        metadata["retry_of"] = retry_of
    
    # If retry_of is provided, replace the old failed batch entry with new batch_id
    if retry_of:
        # Delete the old batch entry and insert new one with new batch_id
        await batch_metadata_collection.delete_one({"batch_id": retry_of})
        await batch_metadata_collection.insert_one(metadata)
    else:
        await batch_metadata_collection.replace_one(
            {"batch_id": batch_id},
            metadata,
            upsert=True
        )


async def resume_batch_processing():
    """
    Resume processing of existing batches (for when script is re-run).
    
    Returns:
        List of batch IDs that need to be monitored
    """
    logger.info("Checking for existing batches...")
    
    # Find all batches that are not yet processed
    unprocessed_batches = await batch_metadata_collection.find({
        "processed": False,
        "status": {"$in": ["completed", "validating", "in_progress", "finalizing"]}
    }).to_list(length=None)
    
    if not unprocessed_batches:
        logger.info("No existing batches to resume")
        return []
    
    batch_ids = [b["batch_id"] for b in unprocessed_batches]
    logger.info(f"Found {len(batch_ids)} existing batches to monitor")
    return batch_ids


async def submit_batches_async(batch_contents: List[Tuple[List[int], str]], max_concurrent: int = int(MAX_CONCURRENT_BATCH_SUBMISSIONS)) -> List[str]:
    """
    Submit multiple batches in parallel with concurrency control.
    
    For each batch: upload file → create batch → store metadata
    
    Args:
        batch_contents: List of (qids, jsonl_content) tuples
        max_concurrent: Maximum concurrent batch submissions
    
    Returns:
        List of batch_ids
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    batch_ids = []
    
    async def submit_single_batch(qids, jsonl_content):
        async with semaphore:
            try:
                # Upload file
                file_id = await upload_batch_file(jsonl_content)
                logger.info(f"Uploaded batch file: {file_id} for QIDs: {qids[:5]}... ({len(qids)} total)")
                
                # Create batch
                batch_id = await create_batch(file_id)
                logger.info(f"Created batch: {batch_id} for QIDs: {qids[:5]}... ({len(qids)} total)")
                
                # Store metadata
                await store_batch_metadata(batch_id, file_id, qids, "validating", retry_of=None)
                
                return batch_id
            except Exception as e:
                logger.error(f"Error submitting batch for QIDs {qids[:5]}...: {e}")
                raise
    
    # Submit all batches
    tasks = [submit_single_batch(qids, jsonl_content) for qids, jsonl_content in batch_contents]
    
    with async_tqdm(total=len(tasks), desc="Submitting batches") as pbar:
        for coro in asyncio.as_completed(tasks):
            try:
                batch_id = await coro
                batch_ids.append(batch_id)
                pbar.update(1)
            except Exception as e:
                logger.error(f"Failed to submit batch: {e}")
                pbar.update(1)
    
    return batch_ids


async def process_batch_results_async(completed_batch_ids: List[str], max_concurrent: int = int(MAX_CONCURRENT_BATCH_PROCESSING), force_reprocess: bool = False):
    """
    Process completed batches concurrently: download results and upload summaries to MongoDB.
    
    Processes multiple batches in parallel with concurrency control. Each batch's summaries
    are uploaded to MongoDB using bulk_write operations for efficiency. Verifies summaries
    exist in MongoDB before marking batch as processed.
    
    Args:
        completed_batch_ids: List of completed batch IDs
        max_concurrent: Maximum concurrent batch processing operations
        force_reprocess: If True, reprocess batches even if marked as processed
    """
    if not completed_batch_ids:
        logger.info("No batches to process")
        return
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_single_batch(batch_id: str):
        """Process a single batch: download results, upload summaries, verify and mark as processed."""
        async with semaphore:
            try:
                # Get batch metadata
                metadata = await batch_metadata_collection.find_one({"batch_id": batch_id})
                if not metadata:
                    logger.warning(f"No metadata found for batch {batch_id}")
                    return
                
                # Check if there are no qids to process or if the batch is already processed
                if not force_reprocess and metadata.get('processed', False):
                    # Verify summaries actually exist before skipping
                    qids = metadata.get('qids', [])
                    if not qids:
                        logger.warning(f"No QIDs found for batch {batch_id}")
                        return

                    existing_summaries = await summary_collection.find(
                        {"qid": {"$in": qids}}
                    ).to_list(length=None)
                    existing_qids = {doc["qid"] for doc in existing_summaries}
                    
                    if len(existing_qids) == len(qids):
                        logger.info(f"Batch {batch_id} already processed and verified, skipping")
                        return
                    
                output_file_id = metadata.get('result_file_id')
                if not output_file_id:
                    logger.warning(f"No output file ID for batch {batch_id}")
                    return
                
                # Download and parse results (in memory only - no files written to disk)
                logger.info(f"Processing batch {batch_id}...")


                results = await download_batch_results(batch_id, output_file_id)

                #### this ^^ function is not working as expected
                
                # Upload summaries to MongoDB in batch (more efficient than one at a time)
                if not results:
                    logger.warning(f"No results found for batch {batch_id}")
                    return

                await batch_upload_summaries(results)
                
                # Verify summaries were actually saved to MongoDB
                qids_in_results = list(results.keys())
                existing_summaries = await summary_collection.find(
                    {"qid": {"$in": qids_in_results}}
                ).to_list(length=None)
                existing_qids = {doc["qid"] for doc in existing_summaries}
                
                if len(existing_qids) == len(qids_in_results):
                    # All summaries verified - mark batch as processed
                    await batch_metadata_collection.update_one(
                        {"batch_id": batch_id},
                        {"$set": {"processed": True}}
                    )
                    logger.info(f"Processed batch {batch_id}: {len(results)} summaries uploaded and verified")
                else:
                    missing_count = len(qids_in_results) - len(existing_qids)
                    logger.error(
                        f"Batch {batch_id}: Only {len(existing_qids)}/{len(qids_in_results)} summaries found in DB after upload. "
                        f"Missing {missing_count} summaries. NOT marking as processed."
                    )
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch_id}: {e}")
                import traceback
                logger.error(traceback.format_exc())
    
    # Process all batches concurrently
    tasks = [process_single_batch(batch_id) for batch_id in completed_batch_ids]
    
    with async_tqdm(total=len(tasks), desc="Processing batches") as pbar:
        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except Exception as e:
                logger.error(f"Unexpected error in batch processing: {e}")
            finally:
                pbar.update(1)

