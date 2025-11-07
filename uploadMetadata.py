import pymongo
import certifi
import os
from dotenv import load_dotenv
from tqdm import tqdm
from utils import *

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URL from environment variable
URL = os.getenv("MONGODB_URL")
if not URL:
    raise ValueError("MONGODB_URL environment variable is not set. Please check your .env file.")

# Mongo setup
client = pymongo.MongoClient(URL, tlsCAFile=certifi.where())
db = client["leetcode_questions"]
collection = db["question_metadata"]

# Create unique index on qid to prevent duplicates
collection.create_index("qid", unique=True)
print("✅ Created unique index on 'qid'")

# Get all slugs
print("📥 Fetching question slugs...")
slugs = getSlugs()
print(f"✅ Found {len(slugs)} questions")

# Get already uploaded qids (for fast skip)
existing_qids = set(collection.distinct("qid"))
print(f"✅ Loaded {len(existing_qids)} existing qids from database")

# Batch upload configuration
batch_size = 100
batch = []

def flush_batch(ignore_batch_size=False):
    """Flush the current batch to MongoDB."""
    global batch
    if not batch:
        return
    if not ignore_batch_size and len(batch) < batch_size:
        return
    
    try:
        collection.insert_many(batch, ordered=False)
        existing_qids.update(d["qid"] for d in batch)
        print(f"✅ Inserted {len(batch)} documents")
    except pymongo.errors.BulkWriteError as e:
        # Count how many were duplicates vs other errors
        duplicates = sum(1 for error in e.details.get("writeErrors", []) 
                        if error.get("code") == 11000)
        if duplicates > 0:
            print(f"⚠️  Skipped {duplicates} duplicate(s), inserted {len(batch) - duplicates} new documents")
            # Add successfully inserted qids to existing_qids
            inserted_qids = {d["qid"] for d in batch}
            inserted_qids -= {error["op"]["qid"] for error in e.details.get("writeErrors", []) 
                             if error.get("code") == 11000}
            existing_qids.update(inserted_qids)
        else:
            print(f"❌ Error inserting batch: {e}")
    except Exception as e:
        print(f"❌ Error inserting batch: {e}")
    
    batch.clear()

# Process each question
print("\n📤 Uploading question metadata...")
for i in tqdm(range(len(slugs)), desc="Processing questions"):
    qid = i + 1  # qid is 1-indexed
    
    # Skip if already exists
    if qid in existing_qids:
        continue
    
    try:
        # Get question metadata
        question = getJsonObjFromQuestion(slugs[i])
        batch.append(question)
        
        # Flush batch when it reaches batch_size
        flush_batch()
    except Exception as e:
        print(f"\n❌ Error processing question {qid} ({slugs[i]}): {e}")
        continue

# Flush any remaining items in batch
flush_batch(ignore_batch_size=True)

# Final statistics
qids_in_db = set(collection.distinct("qid"))
expected_qids = set(range(1, len(slugs) + 1))
missing_qids = expected_qids - qids_in_db

print("\n" + "="*50)
print("📊 Upload Summary")
print("="*50)
print(f"Total documents in DB: {len(qids_in_db)}")
print(f"Expected: {len(expected_qids)}")
print(f"Missing count: {len(missing_qids)}")
if missing_qids:
    print(f"Missing qids (first 50): {sorted(list(missing_qids))[:50]}")
else:
    print("✅ All questions uploaded successfully!")
print("="*50)

