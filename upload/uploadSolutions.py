from utils import *
from tqdm import tqdm
import pymongo, certifi, re, os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URL from environment variable
URL = os.getenv("MONGODB_URL")
if not URL:
    raise ValueError("MONGODB_URL environment variable is not set. Please check your .env file.")

# Mongo set up
client = pymongo.MongoClient(URL, tlsCAFile=certifi.where())
db = client["leetcode_questions"]

collections = {
    "Python": db["python_solutions"],
    "Java": db["java_solutions"],
    "C++": db["cpp_solutions"],
}

for lang, col in collections.items():
    col.create_index("qid", unique=True)

# Global variables
batch_size = 200
batches = {"Python": [], "Java": [], "C++": []}
directory = "/Users/anshvijay/Desktop/LeetCode/solutions"
lang_map = {"py": "Python", "java": "Java", "cpp": "C++"}

# Helper function(s)
def flush_batches(ignore_batch_size=False):
    for lang, docs in batches.items():
        if not docs:
            continue
        if not ignore_batch_size and len(docs) < batch_size:
            continue
        
        try:
            collections[lang].insert_many(docs, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            print(f"⚠️ Duplicate(s) skipped for {lang}")
        except Exception as e:
            print(f"❌ Error inserting {lang} docs: {e}")

        existing_qids[lang].update(d["qid"] for d in docs)
        batches[lang].clear()

# before starting main loop, get already uploaded qid's
# Get already uploaded qids (for fast skip)
# ".distinct" is redundent technically
existing_qids = {lang: set(col.distinct("qid")) for lang, col in collections.items()}
print({k: len(v) for k, v in existing_qids.items()})
print("✅ Loaded existing qid sets")

for name in tqdm(os.listdir(directory), desc="Scanning directories"):

    # Match directories that start with a number followed by a dot and space
    match = re.match(r"^(\d+)\.", name)
    if not match:
        continue

    subdirectory_path = os.path.join(directory, name)
    if not os.path.isdir(subdirectory_path):
        continue
    
    files_in_subdirectory = os.listdir(subdirectory_path)

    # keep track of question and solutions
    sols = {"Python": [], "Java": [], "C++": []}
    qid = int(name.split(".")[0])

    for file_name in files_in_subdirectory:
        
        # 1) Parts check
        parts = file_name.split(".")
        if len(parts) < 2:
            continue
        
        # 2) Language check
        language = lang_map.get(parts[1], "")
        if not language:
            continue

        # 3) Duplicate check for language
        if int(qid) in existing_qids[language]:
            continue

        # 4) add to correct bodies list
        p = os.path.join(subdirectory_path, file_name)
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            code_text = f.read()
            
        sols[language].append(code_text)
       
    # after populating every solution body, form docs
    for lang, sol in sols.items():
        doc = {
            "qid": int(qid),
            "code": sol,  # List[str]
            "language": lang,
        }
        batches[lang].append(doc)

    flush_batches()
    
# clear any left
flush_batches(ignore_batch_size=True)
print("✅ Upload complete!")
