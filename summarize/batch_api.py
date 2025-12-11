"""
Batch API module for OpenAI Batch API operations.

This module handles:
- Creating JSONL batch files
- Uploading files to OpenAI
- Creating batch jobs
- Downloading batch results
"""

import json
import asyncio
from io import BytesIO
from typing import List, Dict, Tuple

from summarize.config import llm_client, logger, BATCH_SIZE
from summarize.summary_prompt import SUMMARY_GENERATION_PROMPT
from utils.utils import extract_qid_from_custom_id, extract_summary_from_result

# don't use this anymore since we are redoing failed batcehs
def create_batch_files(problems: List[Dict], batch_size: int = int(BATCH_SIZE)) -> List[Tuple[List[int], str]]:
    """
    Create JSONL content in memory for batch processing.
    This is a synchronous function as it only performs in-memory data transformation.
    
    Args:
        problems: List of dicts with 'qid' and 'problem_data_text' keys
        batch_size: Number of requests per batch
    
    Returns:
        List of tuples: (list of qids in batch, JSONL content string)
    """
    batches = []
    
    for i in range(0, len(problems), batch_size):
        batch_problems = problems[i:i + batch_size]
        qids = [p['qid'] for p in batch_problems]
        
        # Create JSONL content
        jsonl_lines = []
        for problem in batch_problems:
            request_data = {
                "custom_id": f"qid-{problem['qid']}",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": "gpt-5.1-nano",
                    "instructions": SUMMARY_GENERATION_PROMPT,
                    "input": f"Problem data:\n\n{problem['problem_data_text']}",
                    "reasoning": {"effort": "medium"},
                    "text": {"verbosity": "low"}
                }
            }
            jsonl_lines.append(json.dumps(request_data))
        
        jsonl_content = "\n".join(jsonl_lines)
        batches.append((qids, jsonl_content))
    
    return batches


async def upload_batch_file(jsonl_content: str) -> str:
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
    file_response = await llm_client.files.create(
        file=file_obj,
        purpose="batch"
    )
    
    return file_response.id

# i don't use this anymore I think
async def create_batch(file_id: str) -> str:
    """
    Create a batch job using the uploaded file.
    
    Args:
        file_id: OpenAI file ID from upload_batch_file
    
    Returns:
        batch_id: OpenAI batch ID
    """
    batch_response = await llm_client.batches.create(
        input_file_id=file_id,
        endpoint="/v1/responses",
        completion_window="24h",
        metadata={
            "description": "LeetCode problem summary generation"
        }
    )
    
    return batch_response.id

# Used when processing completed, but not processed batches
async def download_batch_results(batch_id: str, output_file_id: str) -> Dict[int, str]:
    """
    Download batch results from OpenAI and parse them.
    
    IMPORTANT: This function downloads content IN MEMORY ONLY - no files are written to disk.
    All content is processed in memory and never saved locally.
    
    Args:
        batch_id: OpenAI batch ID
        output_file_id: OpenAI output file ID
    
    Returns:
        Dict mapping qid to summary text
    """
    try:
        # Download file content IN MEMORY ONLY - no disk writes
        # OpenAI SDK returns file content - handle both streaming and non-streaming responses
        file_response = await llm_client.files.content(output_file_id)
        content = file_response.text

        # Parse JSONL (in memory)
        results = {}
        for line in content.strip().split('\n'):
            if not line:
                continue
            try:
                result_data = json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing JSON line: {e}")
                continue
            
            # Extract QID from custom_id
            custom_id = result_data.get('custom_id', '')
            qid = extract_qid_from_custom_id(custom_id)
            if qid is None:
                logger.debug(f"Skipping line with invalid custom_id: {custom_id}")
                continue
            
            # Extract summary from result data
            summary = extract_summary_from_result(result_data, qid)
            if summary:
                results[qid] = summary
        
        # Content is automatically garbage collected - no files on disk to delete
        return results
    except Exception as e:
        logger.error(f"Error downloading results for batch {batch_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}

