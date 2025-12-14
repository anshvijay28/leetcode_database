import asyncio
import json
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from summarize.batch_api import upload_batch_file
from embeddings.config import sync_embeddings_client


async def test_batch_embeddings() -> None:
    """Create a single-line JSONL, upload it, and start a batch embeddings job."""
    text_to_embed = "**1. Problem Essence**\nThis database problem models a social network where users have friendships and “like” pages. You are asked to recommend pages to a specific user based on what their friends like, excluding pages the user already likes, and typically to aggregate how many friends like each recommended page and return them in a particular sorted order. The core challenge is to correctly combine multiple tables, handle bidirectional friendships, exclude disqualified pages, and perform grouping and ordering in a single SQL query."

    # supposed jsonl file
    jsonl_lines = []

    payload = {
        "custom_id": "testing single chunk for two sum",
        "method": "POST",
        "url": "/v1/embeddings",
        "body": {
            "input": text_to_embed,
            "model": "text-embedding-3-large",
            "encoding_format": "float",
            "dimensions": 1024,
        },
    }

    jsonl_lines.append(json.dumps(payload))
    jsonl_content = "\n".join(jsonl_lines)

    file_id = await upload_batch_file(jsonl_content)

    batch = sync_embeddings_client.batches.create(
        input_file_id=file_id,
        endpoint="/v1/embeddings",
        completion_window="24h",
        metadata={"description": "single_line_test_embedding"},
    )


    print(f"Batch created with id: {batch.id} (status: {batch.status})")



asyncio.run(test_batch_embeddings())
