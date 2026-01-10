"""
Format question data into RAG documents.

This module combines question summaries and metadata into formatted documents
for RAG context.
"""

from typing import Dict, Any, List


def format_question_document(question_data: Dict[str, Any]) -> str:
    """
    Format a single question's summary and metadata into a document.
    
    Args:
        question_data: Dictionary with 'qid', 'summary', and 'metadata' keys
    
    Returns:
        Formatted document string
    """
    qid = question_data.get("qid")
    summary = question_data.get("summary", "No summary available")
    metadata = question_data.get("metadata", {})
    
    # Extract relevant metadata fields
    title = metadata.get("title", "Unknown")
    difficulty = metadata.get("difficulty", "Unknown")
    topics = metadata.get("topics", [])
    hints = metadata.get("hints", [])
    question_body = metadata.get("question_body", "")
    
    # Format topics
    topics_str = ", ".join(topics) if topics else "None"
    
    # Format hints
    hints_str = "\n".join(f"  - {hint}" for hint in hints) if hints else "  None"
    
    # Build document
    doc = f"""Question ID: {qid}
Title: {title}
Difficulty: {difficulty}
Topics: {topics_str}

Summary:
{summary}

Question Body:
{question_body}

Hints:
{hints_str}
"""
    return doc


def format_multiple_documents(questions_data: Dict[int, Dict[str, Any]]) -> List[str]:
    """
    Format multiple questions into documents.
    
    Args:
        questions_data: Dictionary mapping qid to question data
    
    Returns:
        List of formatted document strings
    """
    documents = []
    for qid, question_data in questions_data.items():
        doc = format_question_document(question_data)
        documents.append(doc)
    return documents


def combine_documents(documents: List[str]) -> str:
    """
    Combine multiple documents into a single context string.
    
    Args:
        documents: List of formatted document strings
    
    Returns:
        Combined context string
    """
    separator = "\n" + "="*80 + "\n"
    return separator.join(documents)
