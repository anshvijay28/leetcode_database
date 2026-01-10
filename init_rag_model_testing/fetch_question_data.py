"""
Fetch question summary and metadata from MongoDB.

This module retrieves question summaries and metadata for RAG context.
"""

from typing import Dict, Any, Optional, List
from pymongo import AsyncMongoClient
import certifi
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
MONGODB_URL = os.getenv("MONGODB_URL")
DATABASE_NAME = "leetcode_questions"
SUMMARIES_COLLECTION = "question_summaries"
METADATA_COLLECTION = "question_metadata"


async def fetch_question_data(qid: int, client: AsyncMongoClient) -> Optional[Dict[str, Any]]:
    """
    Fetch summary and metadata for a question.
    
    Args:
        qid: Question ID
        client: MongoDB async client
    
    Returns:
        Dictionary with summary and metadata, or None if not found
    """
    db = client[DATABASE_NAME]
    summaries_collection = db[SUMMARIES_COLLECTION]
    metadata_collection = db[METADATA_COLLECTION]
    
    # Fetch summary
    summary_doc = await summaries_collection.find_one({"qid": qid})
    summary = summary_doc.get("summary") if summary_doc else None
    
    # Fetch metadata
    metadata_doc = await metadata_collection.find_one({"qid": qid})
    
    if not metadata_doc:
        return None
    
    return {
        "qid": qid,
        "summary": summary,
        "metadata": metadata_doc
    }


async def fetch_multiple_question_data(qids: List[int], client: AsyncMongoClient) -> Dict[int, Dict[str, Any]]:
    """
    Fetch summaries and metadata for multiple questions.
    
    Args:
        qids: List of question IDs
        client: MongoDB async client
    
    Returns:
        Dictionary mapping qid to question data
    """
    results = {}
    for qid in qids:
        data = await fetch_question_data(qid, client)
        if data:
            results[qid] = data
    return results
