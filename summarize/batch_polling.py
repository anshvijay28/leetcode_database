"""
Batch polling module for checking batch status.

This module handles:
- Polling single batch status
- Polling multiple batches concurrently
"""

from typing import Tuple, Optional
from datetime import datetime

from summarize.config import logger, batch_metadata_collection, llm_client, POLL_INTERVAL
import asyncio


async def poll_until_terminal(batch_id: str) -> str:
    """
    Poll a batch until it reaches a terminal state.

    Returns:
        Final status string.
    """
    terminal_states = {"completed", "failed", "expired", "cancelled", "error"}
    status = "unknown"

    while True:
        status, _ = await poll_batch_status(batch_id)
        if status in terminal_states:
            logger.info(f"Batch {batch_id} reached terminal state: {status}")
            return status
        logger.info(f"Batch {batch_id} status={status}; sleeping {POLL_INTERVAL}s before next poll")
        await asyncio.sleep(int(POLL_INTERVAL))

async def poll_batch_status(batch_id: str) -> Tuple[str, Optional[str]]:
    """
    Poll batch status from OpenAI.
    
    Args:
        batch_id: OpenAI batch ID
    
    Returns:
        Tuple of (status, output_file_id if completed)
    """
    try:
        batch = await llm_client.batches.retrieve(batch_id)
        status = batch.status
        output_file_id = batch.output_file_id if hasattr(batch, 'output_file_id') and batch.output_file_id else None
        
        # Update MongoDB
        await batch_metadata_collection.update_one(
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