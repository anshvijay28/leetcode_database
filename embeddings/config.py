from dotenv import load_dotenv
from pymongo import AsyncMongoClient, MongoClient
import certifi
from openai import AsyncOpenAI, OpenAI
import os
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

# ============================================================================
# Environment Variables
# ============================================================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MONGODB_URL = os.getenv("MONGODB_URL")

if not OPENAI_API_KEY:
    raise ValueError(
        "OPENAI_API_KEY environment variable is not set. "
        "Please set it in your .env file or export it before running."
    )

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
log_filename = os.path.join(LOG_DIR, f"embeddings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

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
# OpenAI EmbeddingsClient
# ============================================================================
async_embeddings_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
sync_embeddings_client = OpenAI(api_key=OPENAI_API_KEY)
# ============================================================================
# MongoDB Client & Collections
# ============================================================================
async_mongo_client = AsyncMongoClient(MONGODB_URL, tlsCAFile=certifi.where())
sync_mongo_client = MongoClient(MONGODB_URL, tlsCAFile=certifi.where())
db = async_mongo_client["leetcode_questions"]

# Problem data collections
collections = {
    "metadata": db["question_metadata"],
    "python": db["python_solutions"],
    "java": db["java_solutions"],
    "cpp": db["cpp_solutions"],
    "summaries": db["question_summaries"],
    "batches": db["summaries_batch_metadata"],
    "chunks": db["chunks"],
}

# Embeddings batch metadata collection
embeddings_batch_metadata_collection = db["embeddings_batch_metadata"]

# Embeddings file metadata collection (for tracking input file uploads)
embeddings_file_metadata_collection = db["embeddings_file_metadata"]

# ============================================================================
# Embedding Model Configuration
# ============================================================================
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 1024

# ============================================================================
# Batch API Configuration
# ============================================================================

EMBEDDING_BATCH_SIZE = "5000"  # Number of chunks per batch
MAX_CONCURRENT_EMBEDDING_BATCH_SUBMISSIONS = "2"
EMBEDDING_POLL_INTERVAL = "60"  # Seconds between polling

# ============================================================================
# Pipeline Configuration
# ============================================================================

# Number of chunks to fetch per iteration (incremental processing)
MAX_CHUNKS_PER_ITERATION = 10000

# Concurrency limits for each pipeline stage
MAX_CONCURRENT_FILE_UPLOADS = 5
MAX_CONCURRENT_FILE_POLLING = 5
MAX_CONCURRENT_BATCH_CREATIONS = 5
MAX_CONCURRENT_BATCH_POLLING = 5
MAX_CONCURRENT_EMBEDDING_UPLOADS = 5