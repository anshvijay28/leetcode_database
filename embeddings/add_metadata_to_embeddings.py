"""
Script to add metadata fields (is_premium, topics, difficulty) to embeddings collection.

This script:
- Reads all embeddings from the embeddings collection
- Looks up corresponding question_metadata for each qid
- Updates each embedding document with is_premium, topics, and difficulty fields
"""

import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv
from pymongo import UpdateOne
from typing import Dict, Set

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from embeddings.config import async_mongo_client, db, logger

load_dotenv()

# Configuration
EMBEDDINGS_COLLECTION = "embeddings"
METADATA_COLLECTION = "question_metadata"
BATCH_SIZE = 10000  # Process embeddings in batches


async def get_metadata_lookup() -> Dict[int, Dict]:
    """
    Build a lookup dictionary mapping qid to metadata fields.
    
    Returns:
        Dictionary mapping qid to {is_premium, topics, difficulty}
    """
    logger.info("Building metadata lookup dictionary...")
    metadata_collection = db[METADATA_COLLECTION]
    
    # Fetch all metadata documents (find() returns AsyncCursor directly, no await needed)
    cursor = metadata_collection.find(
        {},
        {"qid": 1, "is_premium_question": 1, "topics": 1, "difficulty": 1}
    )
    
    metadata_lookup = {}
    async for doc in cursor:
        qid = doc.get("qid")
        if qid is not None:
            metadata_lookup[qid] = {
                "is_premium": doc.get("is_premium_question", False),
                "topics": doc.get("topics", []),
                "difficulty": doc.get("difficulty", "Unknown")
            }
    
    logger.info(f"Built metadata lookup for {len(metadata_lookup)} questions")
    return metadata_lookup


async def update_embeddings_with_metadata() -> None:
    """
    Update all embeddings with metadata fields from question_metadata.
    """
    embeddings_collection = db[EMBEDDINGS_COLLECTION]
    
    # Get metadata lookup
    metadata_lookup = await get_metadata_lookup()
    
    if not metadata_lookup:
        logger.warning("No metadata found. Cannot update embeddings.")
        return
    
    # Track statistics
    total_updated = 0
    total_skipped = 0
    missing_metadata = 0
    total_processed = 0
    
    # Process embeddings in batches
    logger.info(f"Processing embeddings in batches of {BATCH_SIZE}...")
    
    # Use cursor to iterate through all embeddings
    cursor = embeddings_collection.find({})
    
    operations = []
    batch_count = 0
    
    async for embedding_doc in cursor:
        total_processed += 1
        qid = embedding_doc.get("qid")
        
        if qid is None:
            logger.debug(f"Skipping embedding with no qid: {embedding_doc.get('_id')}")
            continue
        
        if qid not in metadata_lookup:
            missing_metadata += 1
            logger.debug(f"No metadata found for qid {qid}")
            continue
        
        metadata = metadata_lookup[qid]
        
        # Check if document already has these fields (skip if all present)
        has_all_fields = (
            "is_premium" in embedding_doc and
            "topics" in embedding_doc and
            "difficulty" in embedding_doc
        )
        
        if has_all_fields:
            total_skipped += 1
            continue
        
        # Create update operation for this specific embedding (qid + chunk_id)
        operations.append(
            UpdateOne(
                {"qid": qid, "chunk_id": embedding_doc.get("chunk_id")},
                {
                    "$set": {
                        "is_premium": metadata["is_premium"],
                        "topics": metadata["topics"],
                        "difficulty": metadata["difficulty"]
                    }
                },
                upsert=False  # Only update existing documents
            )
        )
        
        # Execute bulk write when batch is full
        if len(operations) >= BATCH_SIZE:
            batch_count += 1
            logger.info(f"Executing batch {batch_count} ({len(operations)} operations)...")
            result = await embeddings_collection.bulk_write(operations, ordered=False)
            total_updated += result.modified_count
            logger.info(
                f"Batch {batch_count} complete: {result.modified_count} updated, "
                f"{result.matched_count - result.modified_count} already had metadata"
            )
            operations = []
    
    # Execute remaining operations
    if operations:
        batch_count += 1
        logger.info(f"Executing final batch {batch_count} ({len(operations)} operations)...")
        result = await embeddings_collection.bulk_write(operations, ordered=False)
        total_updated += result.modified_count
        logger.info(
            f"Final batch complete: {result.modified_count} updated, "
            f"{result.matched_count - result.modified_count} already had metadata"
        )
    
    # Summary
    logger.info("=" * 80)
    logger.info("Update Summary:")
    logger.info(f"  Total embeddings processed: {total_processed}")
    logger.info(f"  Total embeddings updated: {total_updated}")
    logger.info(f"  Total embeddings skipped (already had metadata): {total_skipped}")
    logger.info(f"  Questions with missing metadata: {missing_metadata}")
    logger.info("=" * 80)


async def main() -> None:
    """Main function."""
    try:
        logger.info("Starting metadata update for embeddings collection...")
        await update_embeddings_with_metadata()
        logger.info("Metadata update complete!")
    except Exception as e:
        logger.error(f"Error updating embeddings: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        await async_mongo_client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
