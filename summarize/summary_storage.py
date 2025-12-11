"""
Summary storage module.

This module handles uploading summaries to MongoDB:
- Single summary uploads
- Batch summary uploads (more efficient)
"""

from typing import Dict
from pymongo import ReplaceOne

from summarize.config import summary_collection, logger


async def async_upload_summary(qid: int, summary: str):
    """
    Upload a single summary to MongoDB (async).
    
    Args:
        qid: Question ID
        summary: Summary text to upload
    """
    summary_data = {"qid": qid, "summary": summary}

    try:
        await summary_collection.replace_one(
            {"qid": qid},
            summary_data,
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error saving summary to DB for QID {qid}: {e}")


async def batch_upload_summaries(summaries: Dict[int, str]):
    """
    Upload multiple summaries to MongoDB in a single batch operation.
    More efficient than uploading one at a time.
    
    Args:
        summaries: Dict mapping qid to summary text
    """
    if not summaries:
        return
    
    try:
        # Use bulk_write with ReplaceOne operations
        # Build operations list efficiently using list comprehension
        operations = [
            ReplaceOne(
                {"qid": qid},
                {"qid": qid, "summary": summary},
                upsert=True
            )
            for qid, summary in summaries.items()
        ]

        # Execute bulk write
        result = await summary_collection.bulk_write(operations, ordered=False)
        logger.info(f"Bulk uploaded {result.upserted_count + result.modified_count} summaries")
    except Exception as e:
        # Fallback to individual uploads if bulk write fails
        logger.warning(f"Bulk write failed, falling back to individual uploads: {e}")
        for qid, summary in summaries.items():
            await async_upload_summary(qid, summary)

