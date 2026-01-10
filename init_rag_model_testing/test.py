"""
Vector search test script for RAG model.

This script:
- Accepts a query string as input
- Generates an embedding using the same model as chunks (text-embedding-3-large)
- Performs vector search to find 5 most similar documents
"""

import asyncio
import sys
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pymongo import AsyncMongoClient
import certifi
from openai import AsyncOpenAI
import os

# Import RAG modules
from fetch_question_data import fetch_multiple_question_data
from format_documents import format_multiple_documents
from rag_llm import generate_rag_response

load_dotenv()

# Configuration
MONGODB_URL = os.getenv("MONGODB_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 1024

# MongoDB configuration
DATABASE_NAME = "leetcode_questions"
EMBEDDINGS_COLLECTION = "embeddings"
CHUNKS_COLLECTION = "chunks"
METADATA_COLLECTION = "question_metadata"
VECTOR_INDEX_NAME = "summary_chunk_embeddings"


async def get_embedding(text: str) -> List[float]:
    """
    Generate embedding for a text string using OpenAI.
    
    Args:
        text: Input text to embed
    
    Returns:
        List of floats representing the embedding vector
    """
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    response = await client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text,
        dimensions=EMBEDDING_DIMENSIONS,
    )
    return response.data[0].embedding


async def get_query_results(query: str, client: AsyncMongoClient) -> List[Dict[str, Any]]:
    """
    Get vector search results for a query string.
    
    Args:
        query: Query string to search for
    
    Returns:
        List of matching documents
    """
    try:
        # Get embedding for the query
        print(f"Generating embedding for query: '{query}'...")
        query_embedding = await get_embedding(query)
        print(f"Embedding generated (dimensions: {len(query_embedding)})")
        
        # Access database and collection
        db = client[DATABASE_NAME]
        embeddings_collection = db[EMBEDDINGS_COLLECTION]
        
        # Vector search aggregation pipeline with join to chunks collection
        pipeline = [
            {
                "$vectorSearch": {
                    "index": VECTOR_INDEX_NAME,
                    "queryVector": query_embedding,
                    "path": "embedding",
                    "exact": True,
                    "limit": 5
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "qid": 1,
                    "chunk_id": 1,
                    "score": {"$meta": "vectorSearchScore"}
                }
            },
            {
                "$lookup": {
                    "from": CHUNKS_COLLECTION,
                    "let": {"embedding_qid": "$qid", "embedding_chunk_id": "$chunk_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$qid", "$$embedding_qid"]},
                                        {"$eq": ["$chunk_id", "$$embedding_chunk_id"]}
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "text": 1
                            }
                        }
                    ],
                    "as": "chunk_data"
                }
            },
            {
                "$unwind": {
                    "path": "$chunk_data",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "qid": 1,
                    "chunk_id": 1,
                    "score": 1,
                    "text": "$chunk_data.text"
                }
            },
            {
                "$lookup": {
                    "from": METADATA_COLLECTION,
                    "let": {"embedding_qid": "$qid"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": ["$qid", "$$embedding_qid"]
                                }
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "title": 1
                            }
                        }
                    ],
                    "as": "metadata"
                }
            },
            {
                "$unwind": {
                    "path": "$metadata",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": CHUNKS_COLLECTION,
                    "let": {"embedding_qid": "$qid"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$qid", "$$embedding_qid"]},
                                        {"$eq": ["$chunk_id", 1]}
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "text": 1
                            }
                        },
                        {"$limit": 1}
                    ],
                    "as": "first_chunk"
                }
            },
            {
                "$unwind": {
                    "path": "$first_chunk",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "qid": 1,
                    "chunk_id": 1,
                    "score": 1,
                    "text": 1,
                    "title": "$metadata.title",
                    "first_chunk_text": "$first_chunk.text"
                }
            }
        ]
        
        print(f"Performing vector search with index '{VECTOR_INDEX_NAME}'...")
        
        # Execute aggregation (aggregate() returns a coroutine that must be awaited)
        cursor = await embeddings_collection.aggregate(pipeline)
        results = []
        async for doc in cursor:
            results.append(doc)
        
        print(f"Found {len(results)} results")
        return results
        
    except Exception as e:
        print(f"Error during vector search: {e}")
        raise


async def main() -> None:
    """Main function to run the vector search test."""
    # Get query from command line argument or use default
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        # Default query for testing
        query = "How do I find two numbers that sum to a target?"
        print(f"No query provided, using default: '{query}'")
        print("Usage: python test.py 'your query here'")
        print()
    
    # Connect to MongoDB and OpenAI
    mongo_client = AsyncMongoClient(MONGODB_URL, tlsCAFile=certifi.where())
    openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    
    try:
        # 1. Perform vector search
        results = await get_query_results(query, mongo_client)
        
        print(f"\n{'='*80}")
        print(f"Vector Search Results for: '{query}'")
        print(f"{'='*80}\n")
        
        if not results:
            print("No results found.")
            return
        
        # Display search results
        qids = []
        for i, doc in enumerate(results, 1):
            problem_title = doc.get('title', 'Unknown')
            qid = doc.get('qid')
            chunk_id = doc.get('chunk_id')
            qids.append(qid)
            print(f"{problem_title} | QID: {qid} | Chunk ID: {chunk_id}")
        
        print(f"{'='*80}")
        print(f"Total results: {len(results)}")
        print(f"{'='*80}\n")
        
        # 2. Fetch question summaries and metadata
        print("Fetching question summaries and metadata...")
        questions_data = await fetch_multiple_question_data(qids, mongo_client)
        print(f"Fetched data for {len(questions_data)} questions\n")
        
        # 3. Format documents
        print("Formatting documents...")
        documents = format_multiple_documents(questions_data)
        print(f"Formatted {len(documents)} documents\n")
        
        # 4. Generate RAG response
        print("Generating RAG response with OpenAI...")
        print(f"{'='*80}")
        rag_response = await generate_rag_response(query, documents, openai_client)
        print(rag_response)
        print(f"{'='*80}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
