"""
Configuration module for the chunks package.

This module handles:
- Environment variable loading
- Logging configuration
- MongoDB client initialization
- Chunking-specific constants and collections
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from pymongo import AsyncMongoClient, MongoClient
import certifi

# Load environment variables
load_dotenv()

# ============================================================================
# Environment Variables
# ============================================================================

MONGODB_URL = os.getenv("MONGODB_URL")

if not MONGODB_URL:
    raise ValueError(
        "MONGODB_URL environment variable is not set. "
        "Please check your .env file."
    )

# ============================================================================
# Logging Configuration
# ============================================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"chunks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# MongoDB Client & Collections
# ============================================================================

async_mongo_client = AsyncMongoClient(MONGODB_URL, tlsCAFile=certifi.where())
sync_mongo_client = MongoClient(MONGODB_URL, tlsCAFile=certifi.where())
db = async_mongo_client["leetcode_questions"]

# Chunks collection
chunks_collection = db["chunks"]

# ============================================================================
# Constants
# ============================================================================

SECTIONS = [
    "1. Problem Essence", 
    "2. Data Structures Used", 
    "3. Algorithms & Techniques Applied",
    "4. Core Insight & Intuition",
    "5. Solution Strategy & Complexity Analysis",
    "6. Pattern Classification",
    "7. Difficulty Analysis",
    "8. Problem Characteristics (Implementation & Cognitive Complexity)",
    "9. Constraints & Their Implications",
    "10. Common Pitfalls & Mistakes",
    "11. Related Concepts & Connections",
    "12. Natural Language Search Terms"  # for RAG Discovery
]  

EXCLUDED_PROBLEMS = [975, 1149, 1398, 752, 2797, 3127, 3160]
