"""
Script to retry all failed batches using the existing OpenAI input files.

High-level behavior:
- Find all documents in batch_metadata with status="failed"
- For each, create a new OpenAI batch using its existing openai_file_id
- Update the SAME batch_metadata document with the new batch_id and reset status/processed
- Track history via previous_batch_ids and retry_count
"""

import sys
import asyncio
import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from tqdm.asyncio import tqdm as async_tqdm

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from summarize.batch_api import upload_batch_file
from summarize.config import logger, mongo_client, batch_metadata_collection, llm_client
from summarize.batch_polling import poll_until_terminal

# get better name for this function
async def rewrite_input_file_to_nano(openai_file_id: str, old_batch_id: str = None) -> Optional[str]:
    """
    Download the existing input JSONL, switch model to gpt-5.1-nano, and upload a new file.

    Returns:
        new_input_file_id if successful, else None.
    """
    try:
        file_resp = await llm_client.files.content(openai_file_id)
        
        lines = file_resp.text.split("\n")

        # Rewrite each JSONL line to force model -> gpt-5.1-nano
        modified_lines = []

        for line in lines:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                body = obj.get("body", {})
                body["model"] = "gpt-5-nano-2025-08-07"
                obj["body"] = body
                modified_lines.append(json.dumps(obj))

            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSONL line during model rewrite")
                continue

        if not modified_lines:
            logger.error(f"No valid JSONL lines after rewrite for batch {old_batch_id}")
            return None

        jsonl_content = "\n".join(modified_lines)

        return jsonl_content
    
    except Exception as e:
        logger.error(f"Error rewriting input file to nano model for batch {old_batch_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# get better name for this function
async def combine_input_files_to_nano(batch_group: List[Dict]) -> str:
    """
    Download input files from multiple batches, rewrite model to gpt-5.1-nano, combine them, and upload as a new file.
    
    Args:
        batch_group: List of batch metadata documents (typically 4, but can be 1-4)
    
    Returns:
        new_input_file_id if successful, else None.
    """
    jsonl_contents = []
    
    for batch_doc in batch_group:
        old_batch_id = batch_doc.get("batch_id")
        openai_file_id = batch_doc.get("openai_file_id")
        
        if not openai_file_id:
            logger.warning(f"Skipping batch {old_batch_id}: missing openai_file_id")
            continue
        
        jsonl_content = await rewrite_input_file_to_nano(openai_file_id, old_batch_id)
        if jsonl_content:
            jsonl_contents.append(jsonl_content)
            logger.info(f"Processed {len(jsonl_content.split("\n"))} lines from batch {old_batch_id}")
        else:
            logger.warning(f"Failed to rewrite input file for batch {old_batch_id}, skipping")
    
    if not jsonl_contents:
        logger.error(f"No valid JSONL content after processing {len(batch_group)} batch(es)")
        return None
    
    # Combine all JSONL contents
    combined_jsonl = "\n".join(jsonl_contents)
    
    return combined_jsonl


async def create_combined_retry_batch(batch_group: List[Dict]) -> Optional[str]:
    """
    Create a combined retry batch from multiple failed batches.
    
    Args:
        batch_group: List of batch metadata documents (1-4 batches to combine)
        combined_jsonl_content: Combined JSONL content string (already rewritten to nano model)
    
    Returns:
        new_batch_id if successful, else None
    """
    if not batch_group:
        logger.error("Cannot create combined batch: batch_group is empty")
        return None
    
    # get metadata from all batches
    old_batch_ids = [batch_doc.get("batch_id") for batch_doc in batch_group]
    old_file_ids = [batch_doc.get("openai_file_id") for batch_doc in batch_group]
    all_qids = [item for sublist in [batch_doc.get("qids", []) for batch_doc in batch_group] for item in sublist]

    
    # get combined jsonl content
    combined_jsonl_content = await combine_input_files_to_nano(batch_group)
    if not combined_jsonl_content:
        logger.error("Failed to combine input files for batches {old_batch_ids}")
        return None

    new_file_id = await upload_batch_file(combined_jsonl_content)


    if not new_file_id:
        logger.error("Failed to upload combined JSONL file")
        return None
    
    # 2) Create new OpenAI batch
    try:
        batch_response = await llm_client.batches.create(
            input_file_id=new_file_id,
            endpoint="/v1/responses",
            completion_window="24h",
            metadata={
                "description": "LeetCode summary retry batch (combined)",
                "combined_from": ",".join(old_batch_ids),
            },
        )
    except Exception as e:
        logger.error(f"Error creating combined retry batch: {e}")

        import traceback
        logger.error(traceback.format_exc())
        return None
    
    new_batch_id = batch_response.id
    logger.info(f"Created combined batch {new_batch_id} from {len(batch_group)} batch(es)")
    
    # 3) Create new batch_metadata document
    new_metadata = {
        "batch_id": new_batch_id,
        "openai_file_id": new_file_id,
        "status": "validating",
        "processed": False,
        "qids": all_qids,
        "created_at": datetime.utcnow(),
        "completed_at": None,
        "combined_batch": True,
        "combined_from": old_batch_ids,
        "combined_from_file_ids": old_file_ids,
    }
    
    try:
        await batch_metadata_collection.insert_one(new_metadata)
        logger.info(f"Created new batch_metadata document for combined batch {new_batch_id}")
    except Exception as e:
        logger.error(f"Error creating batch_metadata document for {new_batch_id} \
        (OpenAI batch created successfully): {e}. Be sure to manually update the \
        metadata collection for this batch.")
    
    # 4) Update all old batch documents to mark as superseded
    for batch_doc in batch_group:
        old_batch_id = batch_doc.get("batch_id")
        if not old_batch_id:
            continue
        
        try:
            await batch_metadata_collection.update_one(
                {"_id": batch_doc["_id"]},
                {
                    "$set": {
                        "status": "superseded",
                        "superseded_by": new_batch_id,
                    }
                }
            )
        except Exception as e:
            logger.warning(f"Error updating batch {old_batch_id} to superseded: {e}")
    
    logger.info(f"Successfully created combined retry batch {new_batch_id} from {len(batch_group)} batch(es)")
    return new_batch_id


async def get_failed_batches() -> List[Dict]:
    """
    Get all failed batches from batch_metadata collection.
    
    Returns:
        List of batch metadata documents with status="failed"
    """
    failed_batches = await batch_metadata_collection.find(
        {"status": "failed"}
    ).to_list(length=None)

    logger.info(f"Found {len(failed_batches)} failed batch document(s) in batch_metadata")
    return failed_batches


async def create_retry_batch(old_doc: Dict) -> Optional[str]:
    """
    Create a retry batch for a single failed batch document.

    This function:
    - Uses the existing OpenAI input file (openai_file_id) to create a new batch
    - Updates the same MongoDB document with the new batch_id and reset status/processed
    - Appends the old batch_id to previous_batch_ids and bumps retry_count
    """
    old_batch_id = old_doc.get("batch_id")
    openai_file_id = old_doc.get("openai_file_id")

    if not openai_file_id:
        logger.error(f"Cannot retry batch {old_batch_id}: missing openai_file_id")
        return None

    logger.info(f"Creating retry batch for failed batch {old_batch_id} using file {openai_file_id}")

    jsonl_content = await rewrite_input_file_to_nano(openai_file_id, old_batch_id) 
    new_input_file_id = await upload_batch_file(jsonl_content)
    if not new_input_file_id:
        logger.error(f"Failed to rewrite input file to nano model for batch {old_batch_id}")
        return None

    # 2) Create a new batch on OpenAI using the rewritten input file
    try:
        batch_response = await llm_client.batches.create(
            input_file_id=new_input_file_id,
            endpoint="/v1/responses",
            completion_window="24h",
            metadata={
                "description": "LeetCode summary retry batch",
                "retry_of": old_batch_id,
            },
        )
    except Exception as e:
        logger.error(f"Error creating retry batch for {old_batch_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

    new_batch_id = batch_response.id

    # 2) Build update operations for MongoDB
    update_doc = {
        "$set": {
            "batch_id": new_batch_id,
            "status": "validating",
            "processed": False,
            "created_at": datetime.utcnow(),
            "completed_at": None,
        },
        "$push": {
            "previous_batch_ids": old_batch_id,
            "previous_openai_file_ids": openai_file_id,
        },
        "$inc": {
            "retry_count": 1,
        },
    }

    # 3) Apply the update to the existing document
    query = {"_id": old_doc["_id"]}
    result = await batch_metadata_collection.update_one(query, update_doc)

    if result.matched_count == 0:
        logger.error(
            f"Retry batch {new_batch_id} created, but failed to update batch_metadata for old batch {old_batch_id}"
        )
    else:
        logger.info(
            f"Created retry batch {new_batch_id} for old batch {old_batch_id} "
            f"and updated batch_metadata (matched={result.matched_count}, modified={result.modified_count})"
        )

    return new_batch_id


async def retry_single_failed_batch(batch_id: Optional[str] = None) -> None:
    """
    Retry a single failed batch.
    
    Args:
        batch_id: Optional batch_id to retry. If None, retries the first failed batch found.
    """
    logger.info("="*80)
    logger.info("RETRYING SINGLE FAILED BATCH")
    logger.info("="*80)
    
    # Find the batch to retry
    if batch_id:
        query = {"batch_id": batch_id, "status": "failed"}
        logger.info(f"Looking for failed batch with batch_id={batch_id}")
    else:
        query = {"status": "failed"}
        logger.info("Looking for first failed batch")
    
    failed_doc = await batch_metadata_collection.find_one(query)
    
    if not failed_doc:
        logger.error(f"No failed batch found matching query: {query}")
        return
    
    old_batch_id = failed_doc.get("batch_id")
    logger.info(f"Found failed batch: {old_batch_id}")
    
    # Retry it 
    new_batch_id = await create_retry_batch(failed_doc)
    if not new_batch_id:
        logger.error("Failed to create retry batch; aborting poll")
        return

    # Always poll until terminal
    await poll_until_terminal(new_batch_id)
    
    logger.info("="*80)
    logger.info("SINGLE BATCH RETRY COMPLETE")
    logger.info("="*80)


async def retry_failed_batches(max_concurrent: int = 2):
    """
    Main function to retry all failed batches by combining them into groups of 4.
    
    Groups batches on-the-fly: accumulates batches into groups of 4, then processes
    each group asynchronously (combines input files, creates batch, polls to terminal).
    """
    logger.info("="*80)
    logger.info("RETRYING FAILED BATCHES (COMBINING INTO GROUPS OF 4)")
    logger.info("="*80)
    
    # Step 1: Get all failed batches
    failed_batches = await get_failed_batches()
    
    if not failed_batches:
        logger.info("No failed batches found to retry")
        return
    
    logger.info(f"Found {len(failed_batches)} failed batch(es); grouping into batches of 4...")

    # Step 2: Group batches on-the-fly and create tasks
    semaphore = asyncio.Semaphore(int(max_concurrent))
    tasks = []
    current_group = []
    GROUP_SIZE = 4

    async def process_group(batch_group: List[Dict]):
        """Process a group of batches: combine, create batch, and poll."""
        async with semaphore:
            new_batch_id = await create_combined_retry_batch(batch_group)
            if not new_batch_id:
                logger.error(f"Failed to create combined retry batch for group of {len(batch_group)} batch(es); skipping poll")
                return
            await poll_until_terminal(new_batch_id)

    # Iterate through failed batches and group them
    for i, batch_doc in enumerate(failed_batches):
        current_group.append(batch_doc)
        
        # Process group when it reaches GROUP_SIZE or we're at the last batch
        if len(current_group) == GROUP_SIZE or i == len(failed_batches) - 1:
            group_size = len(current_group)
            logger.info(f"Processing group of {group_size} batch(es) (batch {i+1}/{len(failed_batches)})")
            task = asyncio.create_task(process_group(current_group.copy()))
            tasks.append(task)
            current_group = []

    if not tasks:
        logger.warning("No batch groups created")
        return

    logger.info(f"Created {len(tasks)} batch group(s); processing with max_concurrent={max_concurrent}")

    # Step 3: Process all groups concurrently
    with async_tqdm(total=len(tasks), desc="Processing batch groups") as pbar:
        for coro in asyncio.as_completed(tasks):
            try:
                await coro # execute the coroutine
            except Exception as e:
                logger.error(f"Unexpected error while processing a batch group: {e}")
            finally:
                pbar.update(1)

    logger.info("=" * 80)
    logger.info("RETRY COMPLETE")
    logger.info("=" * 80)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Retry failed batches")
    parser.add_argument(
        "--single",
        metavar="BATCH_ID",
        nargs="?",
        const=None,
        help="Retry a single failed batch. If BATCH_ID is provided, retry that specific batch. If omitted, retry the first failed batch found."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Retry all failed batches (default behavior)"
    )
    
    args = parser.parse_args()
    
    try:
        if args.single is not None:
            # Retry single batch mode
            batch_id = args.single if args.single else None
            await retry_single_failed_batch(batch_id)
        else:
            # Default: retry all failed batches
            await retry_failed_batches(max_concurrent=2)
    except Exception as e:
        logger.error(f"Error retrying failed batches: {e}", exc_info=True)
    finally:
        await mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())

