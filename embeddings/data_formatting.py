"""
Data formatting module for embeddings.

This module handles:
- Converting chunks to JSONL format for OpenAI batch API
"""

import json
from typing import List, Dict, Tuple

from embeddings.config import EMBEDDING_MODEL, EMBEDDING_DIMENSIONS, EMBEDDING_BATCH_SIZE


def chunks_to_jsonl_content(chunks: List[Dict], batch_size: int = None) -> List[Tuple[List[Tuple[int, int]], str]]:
    """
    Create JSONL content in memory for batch embedding processing.
    This is a synchronous function as it only performs in-memory data transformation.
    
    Args:
        chunks: List of chunk dicts [{"qid": int, "chunk_id": int, "text": str}, ...]
        batch_size: Number of requests per batch (defaults to EMBEDDING_BATCH_SIZE)
    
    Returns:
        List of tuples: (list of (qid, chunk_id) tuples in batch, JSONL content string)
    """
    if batch_size is None:
        batch_size = int(EMBEDDING_BATCH_SIZE)
    
    batches = []
    
    for i in range(0, len(chunks), batch_size):
        batch_chunks = chunks[i:i + batch_size]
        chunk_ids = [(chunk["qid"], chunk["chunk_id"]) for chunk in batch_chunks]
        
        # Create JSONL content
        jsonl_lines = []
        for chunk in batch_chunks:
            request_data = {
                "custom_id": f"qid-{chunk['qid']}-chunk-{chunk['chunk_id']}",
                "method": "POST",
                "url": "/v1/embeddings",
                "body": {
                    "input": chunk["text"],
                    "model": EMBEDDING_MODEL,
                    "encoding_format": "float",
                    "dimensions": EMBEDDING_DIMENSIONS,
                },
            }
            jsonl_lines.append(json.dumps(request_data))
        
        jsonl_content = "\n".join(jsonl_lines)
        batches.append((chunk_ids, jsonl_content))
    
    return batches
