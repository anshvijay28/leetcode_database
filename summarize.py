import getpass
import os
import sys
import time
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from tqdm import tqdm
import pymongo
import certifi
from anthropic import Anthropic
from utils import extract_constraints_and_followup
from summary_prompt import SUMMARY_GENERATION_PROMPT

load_dotenv()

# Configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MONGODB_URL = os.getenv("MONGODB_URL")

if not ANTHROPIC_API_KEY:
    ANTHROPIC_API_KEY = getpass.getpass("Enter your Anthropic API key: ")

if not MONGODB_URL:
    raise ValueError("MONGODB_URL environment variable is not set. Please check your .env file.")

# Initialize Anthropic client
llm_client = Anthropic(api_key=ANTHROPIC_API_KEY)

# MongoDB setup
mongo_client = pymongo.MongoClient(MONGODB_URL, tlsCAFile=certifi.where())
db = mongo_client["leetcode_questions"]

# Collections
collections = {
    "metadata": db["question_metadata"],
    "python": db["python_solutions"],
    "java": db["java_solutions"],
    "cpp": db["cpp_solutions"],
}

# Summary collection
summary_collection = db["question_summaries"]
summary_collection.create_index("qid", unique=True)


def fetch_problem_data(qid) -> dict:
    """Fetch all data for a problem from all 4 collections."""
    data = {"qid": qid}
    
    # Fetch metadata
    metadata = collections["metadata"].find_one({"qid": qid})
    if metadata:
        data["metadata"] = metadata
    
    # Fetch solutions for each language
    for lang in ["python", "java", "cpp"]:
        solution = collections[lang].find_one({"qid": qid})
        if solution:
            data[lang] = solution
    
    return data


def format_problem_data(data):
    """Format problem data into a readable string for the prompt."""
    formatted = []
    
    # Metadata
    if "metadata" in data:
        meta = data["metadata"]
        
        # Title
        if meta.get('title'):
            formatted.append("=== TITLE ===")
            formatted.append(meta.get('title'))
            formatted.append("")
        
        # Difficulty
        if meta.get('difficulty'):
            formatted.append("=== DIFFICULTY ===")
            formatted.append(meta.get('difficulty'))
            formatted.append("")
        
        # Topics
        if meta.get('topics'):
            formatted.append("=== TOPICS ===")
            formatted.append(', '.join(meta.get('topics', [])))
            formatted.append("")
        
        # Parse question body to extract constraints and follow-up
        question_body = meta.get('question_body', '')
        main_question, constraints, follow_up = extract_constraints_and_followup(question_body)
        
        # Question Body (without constraints and follow-up)
        if main_question:
            formatted.append("=== QUESTION ===")
            formatted.append(main_question)
            formatted.append("")
        
        # Constraints
        if constraints:
            formatted.append("=== CONSTRAINTS ===")
            formatted.append(constraints)
            formatted.append("")
        
        # Follow-up (from question body)
        if follow_up:
            formatted.append("=== FOLLOW-UP ===")
            formatted.append(follow_up)
            formatted.append("")
        
        # Hints
        if meta.get('hints'):
            formatted.append("=== HINTS ===")
            for i, hint in enumerate(meta.get('hints', []), 1):
                formatted.append(f"{i}. {hint}")
            formatted.append("")
        
        # Code Stub
        if meta.get('code_stub'):
            formatted.append("=== CODE TEMPLATE ===")
            formatted.append(meta.get('code_stub'))
            formatted.append("")
        
        # Similar Questions (separate from follow-up in question body)
        if meta.get('similar_questions'):
            formatted.append("=== SIMILAR QUESTIONS ===")
            similar = meta.get('similar_questions', [])
            if isinstance(similar, list):
                formatted.append(', '.join(str(q) for q in similar))
            else:
                formatted.append(str(similar))
            formatted.append("")
        
        # RIP: I don't have leetcode premium so I can't get the companies
        # Companies (if premium)
        # if meta.get('companies'):
        #     formatted.append("=== COMPANIES ===")
        #     formatted.append(', '.join(meta.get('companies', [])))
        #     formatted.append("")
    
    # Solutions
    for lang in ["python", "java", "cpp"]:
        if lang in data:
            sol = data[lang]
            formatted.append(f"=== {lang.upper()} SOLUTIONS ===")
            code_list = sol.get("code", [])
            if code_list:
                for i, code in enumerate(code_list, 1):
                    formatted.append(f"\nSolution {i}:\n{code}")
            else:
                formatted.append("No solutions available")
            formatted.append("")
    
    return "\n".join(formatted)


def generate_summary(problem_data_text, model="claude-sonnet-4-5-20250929", max_tokens=4096):
    """Generate a summary using Anthropic API with retry logic."""
    
    user_prompt = f"Problem data:\n\n{problem_data_text}"
    system_prompt = SUMMARY_GENERATION_PROMPT

    messages = [{ "role": "user", "content": user_prompt }]
    
    # Retry logic
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            response = llm_client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=messages,
                system=system_prompt,
                temperature=0,
            )
            return response.content[0].text

        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                print(f"  ⚠️  Error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Failed after {max_retries} attempts: {e}")


def save_summary(qid, summary, save_to_db=True, output_file=None):
    """Save summary to MongoDB and/or file."""
    summary_data = {
        "qid": qid,
        "summary": summary
    }
    
    if save_to_db:
        try:
            summary_collection.replace_one(
                {"qid": qid},
                summary_data,
                upsert=True
            )
        except Exception as e:
            print(f"  ⚠️  Error saving to DB: {e}")
    
    if output_file:
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"QID: {qid}\n")
            f.write(f"{'='*80}\n")
            f.write(summary)
            f.write("\n")


def main():
    """Main function to generate summaries for all problems."""
    # Get prompt file path
    
    # Get all unique qids from all collections
    all_qids = set()
    for name, col in collections.items():

        # only want to generate summaries for questions with metadata and solutions
        if name == "metadata":
            continue

        qids = col.distinct("qid")
        all_qids.update(qids)
    
    all_qids = sorted(all_qids)
    print(f"✅ Found {len(all_qids)} unique problems in database")
    
    # Check which summaries already exist
    existing_summaries = set(summary_collection.distinct("qid"))
    print(f"✅ Found {len(existing_summaries)} existing summaries")
    
    # Ask user if they want to regenerate existing summaries
    regenerate = False
    if existing_summaries:
        response = input("Regenerate existing summaries? (y/n): ").strip().lower()
        regenerate = response == "y"
    
    # Ask for output file (optional)
    output_file = None
    save_file = input("Save summaries to a file? Enter filename (or press Enter to skip): ").strip()
    if save_file:
        output_file = save_file
        # Clear file if it exists and we're regenerating
        if regenerate and os.path.exists(output_file):
            os.remove(output_file)
    
    # Process each problem
    print("\n📝 Generating summaries...")
    successful = 0
    failed = 0
    skipped = 0
    
    for qid in tqdm(all_qids, desc="Summarizing problems"):
        # Skip if summary exists and not regenerating
        if qid in existing_summaries and not regenerate:
            skipped += 1
            continue
        
        try:
            # Fetch problem data
            problem_data = fetch_problem_data(qid)
            
            # Check if we have any data
            if not any(key != "qid" for key in problem_data.keys()):
                print(f"  ⚠️  No data found for QID {qid}, skipping...")
                skipped += 1
                continue
            
            # Format problem data
            problem_data_text = format_problem_data(problem_data)
            
            # Generate summary
            summary = generate_summary(problem_data_text)
            
            # Save summary
            save_summary(qid, summary, save_to_db=True, output_file=output_file)
            
            successful += 1
            
        except Exception as e:
            print(f"  ❌ Error processing QID {qid}: {e}")
            failed += 1
            continue
    
    # Summary statistics
    print("\n" + "="*80)
    print("📊 Summary Generation Complete")
    print("="*80)
    print(f"✅ Successful: {successful}")
    print(f"⚠️  Skipped: {skipped}")
    print(f"❌ Failed: {failed}")
    print(f"📁 Total processed: {successful + failed}")
    if output_file:
        print(f"💾 Summaries saved to: {output_file}")
    print("="*80)


if __name__ == "__main__":
    # lett test it first with a single question

    problem_data = fetch_problem_data(1611)
    problem_data_text = format_problem_data(problem_data)
    summary = generate_summary(problem_data_text)
    print(summary)


    # main()
