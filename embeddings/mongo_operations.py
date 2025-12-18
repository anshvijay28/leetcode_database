"""
MongoDB operations module for embeddings.

This module handles:
- Fetching chunks without batches
- Storing batch metadata
- Getting incomplete batch IDs
- Uploading embeddings to MongoDB
"""

from typing import List, Dict, Tuple
from datetime import datetime
from pymongo import ReplaceOne

from embeddings.config import (
    logger,
    collections,
    embeddings_batch_metadata_collection,
    embeddings_file_metadata_collection,
    db,
)

# this is still pretty inefficient, but idrc enough to fix it
async def get_chunks_without_batches(limit: int = 10000) -> List[Dict]:
    """
    Get chunks from MongoDB that don't have corresponding batches yet.
    Limited to `limit` chunks for incremental processing.

    This implementation pushes the exclusion logic into MongoDB using an
    aggregation pipeline with $lookup and $expr, instead of loading all
    batch metadata into memory and diffing in Python.

    Args:
        limit: Maximum number of chunks to return (default: 10000)

    Returns:
        List of chunk documents: [{"qid": int, "chunk_id": int, "text": str}, ...]
    """
    logger.info(f"Fetching up to {limit} chunks without batches via aggregation...")

    pipeline = [
        {
            "$lookup": {
                "from": "embeddings_batch_metadata",
                "let": {"qid": "$qid", "chunk_id": "$chunk_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                # Check if [qid, chunk_id] appears in batch.chunk_ids
                                "$in": [["$$qid", "$$chunk_id"], "$chunk_ids"]
                            }
                        }
                    }
                ],
                "as": "batches",
            }
        },
        # Keep only chunks that have NO matching batch (batches array is empty)
        {"$match": {"batches": {"$size": 0}}},
        # Limit for incremental processing
        {"$limit": limit},
        # Only return the fields we care about
        {"$project": {"qid": 1, "chunk_id": 1, "text": 1, "_id": 0}},
    ]

    # aggregate(...) is async in the PyMongo async client, so we must await it
    cursor = await collections["chunks"].aggregate(pipeline)
    chunks_without_batches: List[Dict] = []

    async for doc in cursor:
        chunks_without_batches.append(doc)

    logger.info(
        f"Aggregation found {len(chunks_without_batches)} chunks without batches "
        f"(limit={limit})"
    )
    return chunks_without_batches


async def store_batch_metadata(batch_id: str, file_id: str, chunk_ids: List[Tuple[int, int]], status: str = "validating", retry_of: str = None):
    """
    Store batch metadata in MongoDB.
    
    Args:
        batch_id: OpenAI batch ID
        file_id: OpenAI file ID
        chunk_ids: List of (qid, chunk_id) tuples in this batch
        status: Current batch status
        retry_of: Optional batch_id of the failed batch this retry replaces
    """
    metadata = {
        "batch_id": batch_id,
        "openai_file_id": file_id,
        "status": status,
        "chunk_ids": chunk_ids,
        "created_at": datetime.utcnow(),
        "processed": False
    }
    
    # Add retry tracking if this is a retry
    if retry_of:
        metadata["retry_of"] = retry_of
    
    # If retry_of is provided, replace the old failed batch entry with new batch_id
    if retry_of:
        # Delete the old batch entry and insert new one with new batch_id
        await embeddings_batch_metadata_collection.delete_one({"batch_id": retry_of})
        await embeddings_batch_metadata_collection.insert_one(metadata)
    else:
        await embeddings_batch_metadata_collection.replace_one(
            {"batch_id": batch_id},
            metadata,
            upsert=True
        )


async def get_incomplete_batch_ids() -> List[str]:
    """
    Get batch IDs that need to be processed.
    
    Includes:
    - Batches that are still in progress (validating, in_progress, finalizing)
    - Batches that are completed but not yet processed (embeddings not uploaded)
    
    Returns:
        List of batch IDs that need to be processed
    """
    logger.info("Checking for incomplete batches...")
    
    # Find batches that need processing:
    # 1. Batches still in progress (not yet completed)
    # 2. Batches that are completed but not yet processed (embeddings not uploaded)
    incomplete_batches = await embeddings_batch_metadata_collection.find({
        "$or": [
            {"status": {"$in": ["validating", "in_progress", "finalizing"]}},
            {"status": "completed", "processed": False}
        ]
    }).to_list(length=None)
    
    if not incomplete_batches:
        logger.info("No incomplete batches found")
        return []
    
    batch_ids = [b["batch_id"] for b in incomplete_batches]
    logger.info(f"Found {len(batch_ids)} incomplete batches")
    return batch_ids


async def store_file_metadata(file_id: str, chunk_ids: List[Tuple[int, int]], status: str = "uploaded") -> None:
    """
    Store file metadata in MongoDB.
    
    Args:
        file_id: OpenAI file ID
        chunk_ids: List of (qid, chunk_id) tuples in this file
        status: Current file status ("uploaded", "processing", "processed", "failed")
    """
    metadata = {
        "file_id": file_id,
        "chunk_ids": chunk_ids,
        "status": status,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    
    await embeddings_file_metadata_collection.replace_one(
        {"file_id": file_id},
        metadata,
        upsert=True
    )


async def update_file_metadata_status(file_id: str, status: str) -> None:
    """
    Update file metadata status in MongoDB.
    
    Args:
        file_id: OpenAI file ID
        status: New status ("uploaded", "processing", "processed", "failed")
    """
    await embeddings_file_metadata_collection.update_one(
        {"file_id": file_id},
        {
            "$set": {
                "status": status,
                "updated_at": datetime.utcnow(),
            }
        }
    )


async def update_batch_metadata_status(batch_id: str, status: str) -> None:
    """
    Update batch metadata status in MongoDB.
    
    Args:
        batch_id: OpenAI batch ID
        status: New status ("validating", "in_progress", "finalizing", "completed", "failed")
    """
    update_data = {
        "$set": {
            "status": status,
        }
    }
    
    # Add completed_at timestamp for terminal states
    if status in ["completed", "failed", "expired", "cancelled"]:
        update_data["$set"]["completed_at"] = datetime.utcnow()
    
    await embeddings_batch_metadata_collection.update_one(
        {"batch_id": batch_id},
        update_data
    )


async def upload_embeddings_to_mongo(embeddings: Dict[Tuple[int, int], List[float]]) -> None:
    """
    Upload embeddings to MongoDB embeddings collection.
    
    Args:
        embeddings: Dictionary mapping (qid, chunk_id) to embedding vector
    """
    if not embeddings:
        logger.warning("No embeddings to upload")
        return
    
    # Get embeddings collection
    embeddings_collection = db["embeddings"]
    
    # Build ReplaceOne operations for bulk write
    operations = []
    for (qid, chunk_id), embedding in embeddings.items():
        operations.append(
            ReplaceOne(
                {"qid": qid, "chunk_id": chunk_id},
                {
                    "qid": qid,
                    "chunk_id": chunk_id,
                    "embedding": embedding
                },
                upsert=True
            )
        )
    
    # Execute bulk write
    if operations:
        result = await embeddings_collection.bulk_write(operations, ordered=False)
        logger.info(
            f"Uploaded {result.upserted_count + result.modified_count} embeddings "
            f"({len(operations)} total)"
        )


async def mark_batch_as_processed(batch_id: str) -> None:
    """
    Mark a batch as processed in MongoDB.
    
    Args:
        batch_id: OpenAI batch ID
    """
    await embeddings_batch_metadata_collection.update_one(
        {"batch_id": batch_id},
        {
            "$set": {
                "processed": True,
                "processed_at": datetime.utcnow(),
            }
        }
    )
    logger.debug(f"Marked batch {batch_id} as processed")
