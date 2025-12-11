# Script to update MongoDB collections with solutions from locally downloaded GitHub repository
# Uses batch updating to append solutions to existing code arrays

import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm
import pymongo
import certifi
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Path to the local repository
REPO_PATH = "/Users/anshvijay/Desktop/leetcode-screenshotter"
TOP_LEVEL_DIR = "editorial_code"

# Language extensions mapping
LANGUAGE_EXTENSIONS = {
    "python": "py",
    "java": "java",
    "cpp": "cpp"
}

# MongoDB language mapping (for collection names)
MONGO_LANG_MAP = {
    "python": "Python",
    "java": "Java",
    "cpp": "C++"
}

# Get MongoDB URL from environment variable
MONGODB_URL = os.getenv("MONGODB_URL")
if not MONGODB_URL:
    raise ValueError("MONGODB_URL environment variable is not set. Please check your .env file.")

# MongoDB setup
mongo_client = pymongo.MongoClient(MONGODB_URL, tlsCAFile=certifi.where())
db = mongo_client["leetcode_questions"]

collections = {
    "Python": db["python_solutions"],
    "Java": db["java_solutions"],
    "C++": db["cpp_solutions"],
}

# Batch update configuration
BATCH_SIZE = 100


def is_solution_file(filename: str) -> Tuple[bool, str]:
    """
    Check if a file is a solution file based on extension.
    
    Returns:
        (is_solution, language) tuple
    """
    filename_lower = filename.lower()
    for lang, ext in LANGUAGE_EXTENSIONS.items():
        if filename_lower.endswith(f".{ext}"):
            return True, lang
    return False, None


def extract_problem_id(dir_name: str) -> Optional[int]:
    """
    Try to extract problem ID from directory name.
    Handles formats like "001. Two Sum", "1000. Problem Name", etc.
    
    This is CRITICAL - must match correctly to avoid attaching wrong solutions!
    """
    # Try to extract number from the beginning of the directory name
    match = re.match(r"^(\d+)", dir_name)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def read_solution_file(file_path: str) -> Optional[str]:
    """Read a solution file and return its content."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return None


def collect_solutions_from_directory(problem_dir_path: str) -> Dict[str, List[str]]:
    """
    Collect all solution files from a problem directory.
    
    Args:
        problem_dir_path: Full path to the problem directory
        problem_dir_name: Name of the problem directory (e.g., "001. Two Sum")
    
    Returns:
        Dictionary mapping language -> list of code strings
    """
    solutions = {"python": [], "java": [], "cpp": []}
    
    if not os.path.isdir(problem_dir_path):
        return solutions
    
    try:
        files = os.listdir(problem_dir_path)
    except Exception:
        return solutions
    
    for filename in files:
        is_solution, lang = is_solution_file(filename)
        if is_solution:
            file_path = os.path.join(problem_dir_path, filename)
            code = read_solution_file(file_path)
            if code:
                solutions[lang].append(code)
    
    return solutions


def traverse_and_collect_solutions(path: str, all_solutions: Dict[int, Dict[str, List[str]]]) -> None:
    """
    Recursively traverse directory structure and collect all solutions.
    
    Args:
        path: Current directory path
        all_solutions: Dictionary to store solutions (qid -> {lang: [code]})
    """
    if not os.path.exists(path) or not os.path.isdir(path):
        return
    
    try:
        items = os.listdir(path)
    except (PermissionError, OSError) as e:
        return
    
    # Separate directories and files
    directories = []
    
    for item in items:
        item_path = os.path.join(path, item)
        try:
            if os.path.isdir(item_path):
                directories.append((item, item_path))
        except (OSError, PermissionError):
            continue
    
    # Determine directory depth to identify problem directories
    # Path format: REPO_PATH/editorial_code/{range}/{problem_dir}
    relative_path = os.path.relpath(path, os.path.join(REPO_PATH, TOP_LEVEL_DIR))
    path_depth = len(Path(relative_path).parts) if relative_path != "." else 0
    
    is_problem_dir = path_depth == 2  # Problem dirs are at depth 2 (range -> problem)
    
    if is_problem_dir:
        # This is a problem directory - extract qid and collect solutions
        dir_name = os.path.basename(path)
        qid = extract_problem_id(dir_name)
        
        if qid is not None:
            # Collect solutions from this directory
            solutions = collect_solutions_from_directory(path)
            
            # Add to all_solutions
            if qid not in all_solutions:
                all_solutions[qid] = {"python": [], "java": [], "cpp": []}
            
            for lang in ["python", "java", "cpp"]:
                if solutions[lang]:
                    all_solutions[qid][lang].extend(solutions[lang])
    
    # Recursively process subdirectories
    for dir_name, dir_path in sorted(directories):
        traverse_and_collect_solutions(dir_path, all_solutions)


def batch_update_solutions(all_solutions: Dict[int, Dict[str, List[str]]]) -> Dict[str, Dict[str, int]]:
    """
    Batch update MongoDB collections with solutions.
    
    Args:
        all_solutions: Dictionary mapping qid -> {lang: [code strings]}
    
    Returns:
        Statistics dictionary
    """
    stats = {
        "Python": {"updated": 0, "created": 0, "errors": 0},
        "Java": {"updated": 0, "created": 0, "errors": 0},
        "C++": {"updated": 0, "created": 0, "errors": 0}
    }
    
    # Prepare batch updates
    batch_updates = {
        "Python": [],
        "Java": [],
        "C++": []
    }
    
    for qid, langs in tqdm(all_solutions.items(), desc="Preparing updates"):
        for lang, code_list in langs.items():
            if code_list:
                mongo_lang = MONGO_LANG_MAP[lang]
                
                # Create update operation: append to code array using $push with $each
                # This appends all codes to the existing array
                # Also set language and qid if creating new document
                update_op = {
                    "$push": {
                        "code": {"$each": code_list}
                    },
                    "$setOnInsert": {
                        "qid": qid,
                        "language": mongo_lang
                    }
                }
                
                batch_updates[mongo_lang].append(
                    pymongo.UpdateOne(
                        {"qid": qid},
                        update_op,
                        upsert=True  # Create document if it doesn't exist
                    )
                )
    
    # Execute batch updates
    for lang, updates in batch_updates.items():
        if not updates:
            continue
        
        collection = collections[lang]
        
        # Process in batches
        for i in tqdm(range(0, len(updates), BATCH_SIZE), desc=f"Updating {lang}"):
            batch = updates[i:i + BATCH_SIZE]
            
            try:
                result = collection.bulk_write(batch, ordered=False)
                stats[lang]["updated"] += result.modified_count
                stats[lang]["created"] += result.upserted_count
            except pymongo.errors.BulkWriteError as e:
                # Handle partial failures
                stats[lang]["errors"] += len(e.details.get("writeErrors", []))
                stats[lang]["updated"] += e.details.get("nModified", 0)
                stats[lang]["created"] += len(e.details.get("upserted", []))
            except Exception:
                stats[lang]["errors"] += len(batch)
    
    return stats




def main():
    """Main function to update MongoDB with solutions from local repository."""
    top_level_path = os.path.join(REPO_PATH, TOP_LEVEL_DIR)
    
    if not os.path.exists(top_level_path):
        print(f"❌ Error: Directory not found: {top_level_path}")
        return
    
    # Collect all solutions
    all_solutions = {}
    try:
        traverse_and_collect_solutions(top_level_path, all_solutions)
    except Exception as e:
        print(f"❌ Error collecting solutions: {e}")
        import traceback
        traceback.print_exc()
        return
    
    if not all_solutions:
        print("❌ No solutions found")
        return
    
    # Count solutions by language
    lang_counts = {"python": 0, "java": 0, "cpp": 0}
    for qid, langs in all_solutions.items():
        for lang in lang_counts.keys():
            lang_counts[lang] += len(langs[lang])
    
    print(f"Found {len(all_solutions)} problems: Python: {lang_counts['python']}, Java: {lang_counts['java']}, C++: {lang_counts['cpp']}")
    
    # Ask for confirmation
    response = input("Proceed with update? (y/n): ").strip().lower()
    if response != "y":
        print("Cancelled")
        return
    
    # Batch update MongoDB
    stats = batch_update_solutions(all_solutions)
    
    # Print summary
    total_updated = sum(s["updated"] for s in stats.values())
    total_created = sum(s["created"] for s in stats.values())
    total_errors = sum(s["errors"] for s in stats.values())
    
    print(f"\nSummary: Updated: {total_updated}, Created: {total_created}", end="")
    if total_errors > 0:
        print(f", Errors: {total_errors}")
    else:
        print()


if __name__ == "__main__":
    main()

