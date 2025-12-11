"""
Data fetching and formatting module.

This module handles:
- Fetching problem data from MongoDB
- Formatting problem data for LLM prompts
"""

from utils.utils import extract_constraints_and_followup
from summarize.config import collections, logger


async def batch_fetch_problem_data(qids: list) -> dict:
    """
    Fetch all data for multiple problems in batch (much more efficient).
    
    This function makes only 4 MongoDB queries total (1 for metadata + 3 for languages)
    instead of 4 queries per problem.
    
    Args:
        qids: List of question IDs
    
    Returns:
        Dictionary mapping qid to problem data dict
    """
    # Batch fetch metadata for all qids at once
    metadata_cursor = collections["metadata"].find({"qid": {"$in": qids}})
    metadata_dict = {}
    async for doc in metadata_cursor:
        metadata_dict[doc["qid"]] = doc
    
    # Batch fetch solutions for each language
    solutions_dict = {lang: {} for lang in ["python", "java", "cpp"]}
    for lang in ["python", "java", "cpp"]:
        cursor = collections[lang].find({"qid": {"$in": qids}})
        async for doc in cursor:
            solutions_dict[lang][doc["qid"]] = doc
    
    # Combine all data
    result = {}
    for qid in qids:
        data = {"qid": qid}
        if qid in metadata_dict:
            data["metadata"] = metadata_dict[qid]
        for lang in ["python", "java", "cpp"]:
            if qid in solutions_dict[lang]:
                data[lang] = solutions_dict[lang][qid]
        result[qid] = data
    
    return result


def format_problem_data(data):
    """
    Format problem data into a readable string for the prompt.
    
    Args:
        data: Dictionary containing problem data (from async_fetch_problem_data)
    
    Returns:
        Formatted string with explicit headers for each section
    """
    formatted = []
    
    # Metadata
    if "metadata" in data:
        meta = data["metadata"]
        
        # Title
        if meta.get('title'):
            formatted.append("=== TITLE ===")
            formatted.append(meta.get('title'))
            formatted.append("")
        
        # Difficulty
        if meta.get('difficulty'):
            formatted.append("=== DIFFICULTY ===")
            formatted.append(meta.get('difficulty'))
            formatted.append("")
        
        # Topics
        if meta.get('topics'):
            formatted.append("=== TOPICS ===")
            formatted.append(', '.join(meta.get('topics', [])))
            formatted.append("")
        
        # Parse question body to extract constraints and follow-up
        question_body = meta.get('question_body', '')
        main_question, constraints, follow_up = extract_constraints_and_followup(question_body)
        
        # Question Body (without constraints and follow-up)
        if main_question:
            formatted.append("=== QUESTION ===")
            formatted.append(main_question)
            formatted.append("")
        
        # Constraints
        if constraints:
            formatted.append("=== CONSTRAINTS ===")
            formatted.append(constraints)
            formatted.append("")
        
        # Follow-up (from question body)
        if follow_up:
            formatted.append("=== FOLLOW-UP ===")
            formatted.append(follow_up)
            formatted.append("")
        
        # Hints
        if meta.get('hints'):
            formatted.append("=== HINTS ===")
            for i, hint in enumerate(meta.get('hints', []), 1):
                formatted.append(f"{i}. {hint}")
            formatted.append("")
        
        # Code Stub
        if meta.get('code_stub'):
            formatted.append("=== CODE TEMPLATE ===")
            formatted.append(meta.get('code_stub'))
            formatted.append("")
        
        # Similar Questions (separate from follow-up in question body)
        if meta.get('similar_questions'):
            formatted.append("=== SIMILAR QUESTIONS ===")
            similar = meta.get('similar_questions', [])
            if isinstance(similar, list):
                formatted.append(', '.join(str(q) for q in similar))
            else:
                formatted.append(str(similar))
            formatted.append("")
    
    # Solutions
    for lang in ["python", "java", "cpp"]:
        if lang in data:
            sol = data[lang]
            formatted.append(f"=== {lang.upper()} SOLUTIONS ===")
            code_list = sol.get("code", [])
            if code_list:
                for i, code in enumerate(code_list, 1):
                    formatted.append(f"\nSolution {i}:\n{code}")
            else:
                formatted.append("No solutions available")
            formatted.append("")
    
    return "\n".join(formatted)

