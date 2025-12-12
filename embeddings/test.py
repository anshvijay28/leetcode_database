import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from embeddings.config import sync_embeddings_client


embedding = sync_embeddings_client.embeddings.create(
  model="text-embedding-3-small",
  input="The food was delicious and the waiter...",
  encoding_format="float"
)

print(embedding)

