"""
Script to extract all unique tags from the question_metadata collection.

This script:
- Iterates through all documents in question_metadata collection
- Extracts all topics/tags from each document
- Builds a set of unique tags
- Outputs the results
"""

import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv
from typing import Set

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from embeddings.config import async_mongo_client, db, logger

load_dotenv()

# Configuration
METADATA_COLLECTION = "question_metadata"


async def get_all_unique_tags() -> Set[str]:
    """
    Extract all unique tags from the question_metadata collection.
    
    Returns:
        Set of unique tag strings
    """
    metadata_collection = db[METADATA_COLLECTION]
    
    logger.info("Extracting unique tags from question_metadata collection...")
    
    unique_tags: Set[str] = set()
    total_documents = 0
    
    # Iterate through all metadata documents
    cursor = metadata_collection.find({}, {"topics": 1})
    
    async for doc in cursor:
        total_documents += 1
        topics = doc.get("topics", [])
        
        if topics:
            # Add all topics to the set
            for topic in topics:
                if topic:  # Skip empty strings
                    unique_tags.add(topic)
    
    logger.info(f"Processed {total_documents} documents")
    logger.info(f"Found {len(unique_tags)} unique tags")
    
    return unique_tags


async def main() -> None:
    """Main function."""
    try:
        unique_tags = await get_all_unique_tags()
        
        # Sort tags for better readability
        sorted_tags = sorted(unique_tags)
        
        # Output results
        print("\n" + "=" * 80)
        print(f"Unique Tags in Database: {len(sorted_tags)}")
        print("=" * 80)
        
        # Print tags in columns for better readability
        for i, tag in enumerate(sorted_tags, 1):
            print(f"{i:4d}. {tag}")
        
        print("\n" + "=" * 80)
        print(f"Total unique tags: {len(sorted_tags)}")
        print("=" * 80)
        
        # Also save to a file
        output_file = "unique_tags.txt"
        with open(output_file, "w") as f:
            for tag in sorted_tags:
                f.write(f"{tag}\n")
        
        logger.info(f"Tags saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error extracting tags: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        await async_mongo_client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
