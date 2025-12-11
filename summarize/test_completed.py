"""
Script to sync in_progress batch statuses from OpenAI to MongoDB.
"""

import sys
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from summarize.config import sync_mongo_client, sync_llm_client


def main():
    db = sync_mongo_client["leetcode_questions"]
    collection = db["batch_metadata"]
    
    # Find all batches with status="in_progress"
    in_progress_batches = list(collection.find({"status": "in_progress"}))
    
    print(f"Found {len(in_progress_batches)} batch(es) with status='in_progress'")
    
    for batch_doc in in_progress_batches:
        batch_id = batch_doc["batch_id"]
        print(f"\nChecking batch: {batch_id}")
        
        try:
            # Get actual status from OpenAI
            batch = sync_llm_client.batches.retrieve(batch_id)
            actual_status = batch.status
            output_file_id = getattr(batch, 'output_file_id', None)
            
            print(f"  OpenAI status: {actual_status}")
            if output_file_id:
                print(f"  Output file ID: {output_file_id}")
            
            # Update MongoDB
            update_doc = {
                "$set": {
                    "status": actual_status,
                    "result_file_id": output_file_id,
                }
            }
            
            # If terminal state, set completed_at
            terminal_states = {"completed", "failed", "expired", "cancelled", "error"}
            if actual_status in terminal_states:
                update_doc["$set"]["completed_at"] = datetime.utcnow()
            
            result = collection.update_one(
                {"_id": batch_doc["_id"]},
                update_doc
            )
            
            print(f"  Updated MongoDB: matched={result.matched_count}, modified={result.modified_count}")
            
        except Exception as e:
            print(f"  ERROR: {e}")
    
    print(f"\nDone! Updated {len(in_progress_batches)} batch(es)")
    sync_mongo_client.close()


if __name__ == "__main__":
    main()