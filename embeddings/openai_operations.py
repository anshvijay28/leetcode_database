"""
OpenAI operations module for embeddings.

This module handles:
- Uploading files to OpenAI
- Polling file status
- Creating batch jobs
- Polling batch status
- Downloading batch results
"""

import json
from io import BytesIO
from typing import Tuple, Optional, Dict, List

from embeddings.config import (
    logger,
    async_embeddings_client,
    embeddings_batch_metadata_collection,
)
from datetime import datetime


async def submit_input_file(jsonl_content: str) -> str:
    """
    Upload JSONL content to OpenAI as a file.
    
    Args:
        jsonl_content: JSONL content as string
    
    Returns:
        file_id: OpenAI file ID
    """
    # Convert string to bytes
    jsonl_bytes = jsonl_content.encode('utf-8')
    
    # Create a file-like object with name attribute (required by OpenAI SDK)
    file_obj = BytesIO(jsonl_bytes)
    file_obj.name = "batch.jsonl"
    
    # Upload file - OpenAI SDK expects a file-like object
    file_response = await async_embeddings_client.files.create(
        file=file_obj,
        purpose="batch"
    )
    
    return file_response.id


async def submit_embedding_batch(file_id: str) -> str:
    """
    Create a batch job using the uploaded file for embeddings.
    
    Args:
        file_id: OpenAI file ID from submit_input_file
    
    Returns:
        batch_id: OpenAI batch ID
    """
    batch_response = await async_embeddings_client.batches.create(
        input_file_id=file_id,
        endpoint="/v1/embeddings",
        completion_window="24h",
        metadata={
            "description": "LeetCode chunk embeddings"
        }
    )
    
    return batch_response.id


async def poll_file_status(file_id: str) -> Tuple[str, bool]:
    """
    Poll file status from OpenAI.
    
    Args:
        file_id: OpenAI file ID
    
    Returns:
        Tuple of (status, is_ready) where is_ready=True when status="processed"
    """
    try:
        file = await async_embeddings_client.files.retrieve(file_id)
        status = file.status
        is_ready = (status == "processed")
        return status, is_ready
    except Exception as e:
        logger.error(f"Error polling file {file_id}: {e}")
        return "error", False


async def poll_batch_status(batch_id: str) -> Tuple[str, Optional[str]]:
    """
    Poll batch status from OpenAI.
    
    Args:
        batch_id: OpenAI batch ID
    
    Returns:
        Tuple of (status, output_file_id if completed)
    """
    try:
        batch = await async_embeddings_client.batches.retrieve(batch_id)
        status = batch.status
        output_file_id = batch.output_file_id if hasattr(batch, 'output_file_id') and batch.output_file_id else None
        
        # Update MongoDB
        await embeddings_batch_metadata_collection.update_one(
            {"batch_id": batch_id},
            {
                "$set": {
                    "status": status,
                    "result_file_id": output_file_id,
                    "completed_at": datetime.utcnow() if status in ["completed", "failed", "expired"] else None
                }
            }
        )
        
        return status, output_file_id
    except Exception as e:
        logger.error(f"Error polling batch {batch_id}: {e}")
        return "error", None


async def download_batch_results(batch_id: str, output_file_id: str) -> Dict[Tuple[int, int], List[float]]:
    """
    Download batch results from OpenAI and parse embeddings.
    
    IMPORTANT: This function downloads content IN MEMORY ONLY - no files are written to disk.
    All content is processed in memory and never saved locally.
    
    Args:
        batch_id: OpenAI batch ID
        output_file_id: OpenAI output file ID
    
    Returns:
        Dict mapping (qid, chunk_id) to embedding vector
    """
    try:
        # Download file content IN MEMORY ONLY - no disk writes
        file_response = await async_embeddings_client.files.content(output_file_id)
        content = file_response.text

        # Parse JSONL (in memory)
        embeddings = {}
        for line in content.strip().split('\n'):
            if not line:
                continue
            try:
                result_data = json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing JSON line: {e}")
                continue
            
            # Extract custom_id and parse qid, chunk_id
            custom_id = result_data.get('custom_id', '')
            if not custom_id.startswith('qid-') or '-chunk-' not in custom_id:
                logger.debug(f"Skipping line with invalid custom_id: {custom_id}")
                continue
            
            # Parse custom_id format: "qid-{qid}-chunk-{chunk_id}"
            try:
                parts = custom_id.replace('qid-', '').split('-chunk-')
                if len(parts) != 2:
                    logger.debug(f"Invalid custom_id format: {custom_id}")
                    continue
                qid = int(parts[0])
                chunk_id = int(parts[1])
            except (ValueError, IndexError) as e:
                logger.debug(f"Error parsing custom_id {custom_id}: {e}")
                continue
            
            # Extract embedding from response body
            response = result_data.get('response', {})
            body = response.get('body', {})
            data = body.get('data', [])
            
            if not data or len(data) == 0:
                logger.warning(f"No embedding data found for {custom_id}")
                continue
            
            # Get embedding vector (first item in data array)
            embedding = data[0].get('embedding', [])
            if not embedding:
                logger.warning(f"Empty embedding for {custom_id}")
                continue
            
            embeddings[(qid, chunk_id)] = embedding
        
        logger.info(f"Downloaded {len(embeddings)} embeddings from batch {batch_id}")
        return embeddings
    except Exception as e:
        logger.error(f"Error downloading results for batch {batch_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}
