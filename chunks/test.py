import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from chunks.database import batch_get_summaries
from chunks.text_processing import chunk_summary


async def get_chunks_for_qid(qid: int) -> list[dict]:
    """
    Get chunks for a given question ID.
    
    Args:
        qid: Question ID
        
    Returns:
        List of chunk documents: [{"qid": qid, "chunk_id": chunk_id, "text": text}, ...]
    """
    # Fetch the summary for the qid
    summaries = await batch_get_summaries([qid])
    
    if qid not in summaries:
        print(f"No summary found for qid {qid}")
        return []
    
    summary = summaries[qid]
    
    # Chunk the summary
    chunks = chunk_summary(qid, summary)
    
    return chunks


async def main():
    """Test function to get chunks for a specific qid."""
    # Example: get chunks for qid 1
    qid = 3127
    chunks = await get_chunks_for_qid(qid)
    
    print(f"Found {len(chunks)} chunks for qid {qid}")
    for chunk in chunks:
        print(f"\nChunk {chunk['chunk_id']}:")
        print(f"QID: {chunk['qid']}")
        print(f"Text preview: {chunk['text']}")


if __name__ == "__main__":
    asyncio.run(main())

