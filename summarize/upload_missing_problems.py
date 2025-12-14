"""
Script to upload summaries for missing problems.

This script:
1. Takes a list of QIDs for missing problems
2. Fetches problem data from MongoDB
3. Creates a single batch for these QIDs
4. Submits the batch synchronously (one batch)
5. Polls until batch completion
6. Uploads summaries to MongoDB
"""

import sys
import json
import asyncio
from pathlib import Path
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

# Ensure project root is on sys.path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from summarize.config import logger, mongo_client, batch_metadata_collection,summary_collection
from summarize.data_fetcher import batch_fetch_problem_data, format_problem_data
from summarize.batch_api import upload_batch_file, create_batch, download_batch_results
from summarize.batch_manager import store_batch_metadata
from summarize.batch_polling import poll_until_terminal, poll_batch_status
from summarize.summary_storage import batch_upload_summaries
from summarize.summary_prompt import SUMMARY_GENERATION_PROMPT


async def upload_missing_problems(qids: List[int]) -> bool:
    """
    Upload summaries for missing problems by creating and processing a batch.
    
    Args:
        qids: List of question IDs to process
    
    Returns:
        True if successful, False otherwise
    """
    if not qids:
        logger.warning("No QIDs provided")
        return False
    
    logger.info(f"Starting upload for {len(qids)} missing problem(s): {qids}")
    
    try:
        # Step 1: Fetch problem data from MongoDB
        logger.info("Fetching problem data from MongoDB...")
        problem_data = await batch_fetch_problem_data(qids)
        
        # Check if all QIDs were found
        missing_qids = [qid for qid in qids if qid not in problem_data]
        if missing_qids:
            logger.error(f"QIDs not found in MongoDB: {missing_qids}")
            return False
        
        # Step 2: Format problem data into text
        logger.info("Formatting problem data...")
        problems = []
        for qid in qids:
            data = problem_data[qid]
            problem_text = format_problem_data(data)
            problems.append({
                'qid': qid,
                'problem_data_text': problem_text
            })
        
        # Step 3: Create JSONL batch content
        logger.info("Creating batch JSONL content...")
        jsonl_lines = []
        for problem in problems:
            request_data = {
                "custom_id": f"qid-{problem['qid']}",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": "gpt-5.1-2025-11-13",
                    "instructions": SUMMARY_GENERATION_PROMPT,
                    "input": f"Problem data:\n\n{problem['problem_data_text']}",
                    "reasoning": {"effort": "medium"},
                    "text": {"verbosity": "low"}
                }
            }
            jsonl_lines.append(json.dumps(request_data))
        
        jsonl_content = "\n".join(jsonl_lines)
        logger.info(f"Created JSONL with {len(jsonl_lines)} requests")
        
        # Step 4: Submit batch (synchronously - one batch)
        logger.info("Uploading batch file to OpenAI...")
        file_id = await upload_batch_file(jsonl_content)
        logger.info(f"Uploaded file: {file_id}")
        
        logger.info("Creating batch job...")
        batch_id = await create_batch(file_id)  # this also "submits" the batch
        logger.info(f"Created batch: {batch_id}")
        
        logger.info("Storing batch metadata...")
        await store_batch_metadata(batch_id, file_id, qids, "validating")
        logger.info(f"Batch {batch_id} submitted successfully")
        
        # Step 5: Poll until completion
        logger.info(f"Polling batch {batch_id} until completion...")
        final_status = await poll_until_terminal(batch_id)
        
        if final_status != "completed":
            logger.error(f"Batch {batch_id} ended with status: {final_status}")
            return False
        
        logger.info(f"Batch {batch_id} completed successfully")
        
        # Step 6: Get result file ID and download results
        status, output_file_id = await poll_batch_status(batch_id)
        if not output_file_id:
            logger.error(f"No output file ID for completed batch {batch_id}")
            return False
        
        logger.info(f"Downloading batch results from file: {output_file_id}")
        results = await download_batch_results(batch_id, output_file_id)
        
        if not results:
            logger.error(f"No results downloaded for batch {batch_id}")
            return False
        
        logger.info(f"Downloaded {len(results)} summaries")
        
        # Step 7: Upload summaries to MongoDB
        logger.info("Uploading summaries to MongoDB...")
        await batch_upload_summaries(results)
        
        # Step 8: Verify summaries were uploaded
        qids_in_results = list(results.keys())
        existing_summaries = await summary_collection.find(
            {"qid": {"$in": qids_in_results}}
        ).to_list(length=None)
        existing_qids = {doc["qid"] for doc in existing_summaries}
        
        if len(existing_qids) == len(qids_in_results):
            # Mark batch as processed
            await batch_metadata_collection.update_one(
                {"batch_id": batch_id},
                {"$set": {"processed": True}}
            )
            logger.info(f"Successfully uploaded {len(results)} summaries to MongoDB")
            logger.info(f"Batch {batch_id} marked as processed")
            return True
        else:
            missing_count = len(qids_in_results) - len(existing_qids)
            logger.error(
                f"Only {len(existing_qids)}/{len(qids_in_results)} summaries found in DB. "
                f"Missing {missing_count} summaries."
            )
            return False
            
    except Exception as e:
        logger.error(f"Error uploading missing problems: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def main():
    """
    Main function for command-line usage.
    Example: python upload_missing_problems.py 1 2 3 20 42
    """
    qids = [1149, 2797, 1398]
    
    await upload_missing_problems(qids)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        # Close MongoDB connection
        asyncio.run(mongo_client.close())
