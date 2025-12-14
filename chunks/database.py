"""
Database operations module for chunks.

This module handles MongoDB operations for fetching summaries and inserting chunks.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
from pymongo import ReplaceOne

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from chunks.config import async_mongo_client, chunks_collection, logger, EXCLUDED_PROBLEMS

async def get_qids_to_process(excluded_qids: list[int] = None) -> list[int]:
    """
    Get all question IDs from the summaries collection, excluding specified qids.
    
    Args:
        excluded_qids: List of qids to exclude. If None, uses EXCLUDED_PROBLEMS from config.
        
    Returns:
        List of question IDs to process
    """
    if excluded_qids is None:
        excluded_qids = EXCLUDED_PROBLEMS
    
    db = async_mongo_client["leetcode_questions"]
    summary_collection = db["question_summaries"]
    
    # Fetch all qids from summaries collection
    cursor = summary_collection.find({}, {"qid": 1, "_id": 0})
    qids = []
    async for doc in cursor:
        qid = doc.get("qid")
        if qid is not None and qid not in excluded_qids:
            qids.append(qid)
    
    return qids

async def batch_get_summaries(qids: list[int]) -> dict[int, str]:
    """
    Get multiple question summaries by qids from MongoDB in a single batch query.
    
    Args:
        qids: List of question IDs
        
    Returns:
        Dictionary mapping qid to summary text. Missing summaries are not included.
    """
    db = async_mongo_client["leetcode_questions"]
    summary_collection = db["question_summaries"]
    
    # Fetch all summaries in a single query
    cursor = summary_collection.find(
        {"qid": {"$in": qids}},
        {"qid": 1, "summary": 1, "_id": 0}
    )
    
    # Build dict mapping qid to summary
    summaries = {}
    async for doc in cursor:
        if "summary" in doc and "qid" in doc:
            summaries[doc["qid"]] = doc["summary"]
    
    return summaries


async def batch_insert_chunks(chunks: list[dict], batch_size: int = 500) -> None:
    """
    Insert or replace chunks in MongoDB using bulk write operations.
    
    Uses ReplaceOne with upsert=True to replace existing chunks or insert new ones.
    Processes chunks in batches for efficiency.
    
    Args:
        chunks: List of chunk documents in format [{"qid": int, "chunk_id": int, "text": str}, ...]
        batch_size: Number of chunks to process per batch (default: 500)
    """
    if not chunks:
        return
    
    try:
        # Process chunks in batches
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:min(i + batch_size, len(chunks))]
            
            # Build ReplaceOne operations for this batch
            operations = [
                ReplaceOne(
                    {"qid": chunk["qid"], "chunk_id": chunk["chunk_id"]},
                    chunk,
                    upsert=True
                )
                for chunk in batch
            ]
            
            # Execute bulk write
            result = await chunks_collection.bulk_write(operations, ordered=False)
            logger.info(
                f"Batch {i // batch_size + 1}: Inserted/replaced "
                f"{result.upserted_count + result.modified_count} chunks "
                f"({len(batch)} total in batch)"
            )
            
    except Exception as e:
        logger.error(f"Error in batch_insert_chunks: {e}")
        raise
