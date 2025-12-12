"""
Script to process all completed batches and upload their summaries.

Steps:
1. Find all batches in batch_metadata with status="completed" (both processed and unprocessed)
2. For processed batches, verify summaries actually exist in MongoDB
3. Retry any batches marked as processed but missing summaries
4. Download outputs via existing batch_manager/process_batch_results_async helper
5. Upload summaries into the question_summaries collection
6. Verify summaries exist before marking batch as processed
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Ensure project root is on sys.path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from summarize.config import (  # noqa: E402
    logger,
    mongo_client,
    batch_metadata_collection,
    summary_collection,
)
from summarize.batch_manager import process_batch_results_async  # noqa: E402
from summarize.batch_api import download_batch_results  # noqa: E402
from summarize.summary_storage import batch_upload_summaries  # noqa: E402


async def process_all_completed():
    """
    Process all completed batches, including retrying batches marked as processed
    but missing summaries in the database.
    """
    # Find all completed batches (both processed and unprocessed)
    all_completed_batches = await batch_metadata_collection.find({
        "status": "completed",
    }).to_list(length=None)

    if not all_completed_batches:
        logger.info("No completed batches found.")
        return

    # Separate batches into unprocessed and processed
    unprocessed_batches = [b for b in all_completed_batches if not b.get('processed', False)]
    processed_batches = [b for b in all_completed_batches if b.get('processed', False)]

    logger.info(f"Found {len(unprocessed_batches)} unprocessed completed batch(es)")
    logger.info(f"Found {len(processed_batches)} processed completed batch(es) - will verify summaries exist")

    # Check processed batches to see if summaries actually exist
    batches_to_retry = []
    for batch in processed_batches:
        qids = batch.get('qids', [])
        if not qids:
            continue
        
        # Check if summaries exist in MongoDB
        existing_summaries = await summary_collection.find(
            {"qid": {"$in": qids}}
        ).to_list(length=None)
        existing_qids = {doc["qid"] for doc in existing_summaries}
        
        if len(existing_qids) < len(qids):
            missing_count = len(qids) - len(existing_qids)
            logger.warning(
                f"Batch {batch['batch_id']}: Marked as processed but {missing_count}/{len(qids)} summaries missing. "
                f"Resetting processed flag and will retry."
            )
            # Reset processed flag so batch will be reprocessed
            await batch_metadata_collection.update_one(
                {"batch_id": batch['batch_id']},
                {"$set": {"processed": False}}
            )
            batches_to_retry.append(batch)
        else:
            logger.debug(f"Batch {batch['batch_id']}: All {len(qids)} summaries verified in database")

    # Combine unprocessed batches and batches that need retry
    all_batches_to_process = unprocessed_batches + batches_to_retry
    batch_ids = [batch["batch_id"] for batch in all_batches_to_process]

    if not batch_ids:
        logger.info("No batches need processing - all completed batches have verified summaries.")
        return

    logger.info(
        f"Processing {len(batch_ids)} batch(es): "
        f"{len(unprocessed_batches)} unprocessed + {len(batches_to_retry)} needing retry"
    )
    logger.info(f"Batch IDs: {batch_ids}")

    # Process batches (downloads results + uploads summaries + verifies + marks processed)
    await process_batch_results_async(batch_ids)


async def main():
    parser = argparse.ArgumentParser(
        description="Process or inspect completed batches."
    )
    parser.add_argument(
        "--process",
        metavar="BATCH_ID",
        help="Process a specific batch ID completely (download and upload all summaries).",
    )
    args = parser.parse_args()

    try:
        if args.process:
            logger.info(f"Processing batch {args.process}...")
            await process_batch_results_async([args.process])
        else:
            await process_all_completed()
    finally:
        await mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())

