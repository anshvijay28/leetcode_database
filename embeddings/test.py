import asyncio
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from embeddings.config import async_mongo_client, db, logger


async def test_mongo_connection() -> None:
    """Test connection to MongoDB using async driver."""
    try:
        # Test connection by listing databases
        logger.info("Testing MongoDB connection...")
        databases = await async_mongo_client.list_database_names()
        logger.info(f"Successfully connected! Available databases: {databases}")
        
        # Test accessing a collection
        test_collection = db["chunks"]
        count = await test_collection.count_documents({})
        logger.info(f"Found {count} documents in 'chunks' collection")
        
        logger.info("MongoDB connection test successful!")
        
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise
    finally:
        # Close the connection
        await async_mongo_client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    asyncio.run(test_mongo_connection())
