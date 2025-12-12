"""
Script to obtain a set of all problems that have summaries uploaded 
to the summary collection in MongoDB.
"""

import sys
import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# Ensure the project root is on the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from summarize.config import sync_mongo_client, sync_llm_client
from utils.utils import extract_qid_from_custom_id, extract_summary_from_result


def get_qids_with_summaries():
    """
    Query the question_summaries collection and return a set of all QIDs
    that have summaries, including QIDs from completed but not processed batches.
    """
    db = sync_mongo_client["leetcode_questions"]
    summary_collection = db["question_summaries"]
    
    # Get all documents from the summary collection
    all_summaries = list(summary_collection.find({}, {"qid": 1, "_id": 0}))
    
    # Extract QIDs into a set
    qids_with_summaries = {doc["qid"] for doc in all_summaries}
    
    print(f"Found {len(qids_with_summaries)} problems with summaries in collection")
    
    # Find missing QIDs in the sequence
    sorted_qids = sorted(qids_with_summaries)
    min_qid = min(sorted_qids)
    max_qid = max(sorted_qids)
    all_qids_in_range = set(range(min_qid, max_qid + 1))
    missing_qids = sorted(all_qids_in_range - qids_with_summaries)
    
    print(f"\nSorted QIDs range: {min_qid} to {max_qid}")
    print(f"Missing QIDs: {missing_qids}")
    print(f"Total missing: {len(missing_qids)}")
    
    return qids_with_summaries


def find_summary_in_output_file(result_file_id: str, qid: int) -> str | None:
    """
    Find a summary for a specific QID within an OpenAI batch output file.
    
    Args:
        result_file_id: The OpenAI output file ID (result_file_id from batch metadata)
        qid: The question ID to find
    
    Returns:
        The summary text for the QID, or None if not found
    """
    try:
        # Download file content from OpenAI
        file_response = sync_llm_client.files.content(result_file_id)
        content = file_response.text
        
        # Parse JSONL line by line
        for line in content.strip().split('\n'):
            if not line:
                continue
            try:
                result_data = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {e}")
                continue
            
            # Extract QID from custom_id
            custom_id = result_data.get('custom_id', '')
            extracted_qid = extract_qid_from_custom_id(custom_id)
            
            # Check if this is the QID we're looking for
            if extracted_qid == qid:
                # Extract and return the summary
                with open("problem20_line.json", "w") as f:
                    json.dump(result_data, f, indent=4)

                summary = extract_summary_from_result(result_data, qid)
                print("--------------------------------")
                return summary
        
        # QID not found in the file
        print(f"QID {qid} not found in output file {result_file_id}")   
        return None
        
    except Exception as e:
        print(f"Error finding summary for QID {qid} in file {result_file_id}: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    get_qids_with_summaries()
