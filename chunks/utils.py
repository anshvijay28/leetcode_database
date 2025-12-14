"""
Utility functions for chunks operations.

This module provides utility functions for querying and processing question IDs.
"""

import sys
import re
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from chunks.config import sync_mongo_client


def find_questions_by_summary_string(search_string: str, has_string: bool) -> list[int]:
    """
    Find question IDs based on whether their summary contains a specific string.
    
    Args:
        search_string: The string to search for in the summary field
        has_string: If True, returns QIDs WITH the string. If False, returns QIDs WITHOUT the string.
    
    Returns:
        List of question IDs (qids) matching the condition
    """
    db = sync_mongo_client["leetcode_questions"]
    summary_collection = db["question_summaries"]
    
    # Escape special regex characters in the search string
    escaped_string = re.escape(search_string)
    
    # Build the query based on has_string flag
    if has_string:
        query = {"summary": {"$regex": escaped_string}}
    else:
        query = {"summary": {"$not": {"$regex": escaped_string}}}
    
    # Query and extract qids
    cursor = summary_collection.find(query, {"qid": 1, "_id": 0})
    qids = [doc["qid"] for doc in cursor]
    
    return qids