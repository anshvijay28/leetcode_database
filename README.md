# LeetCode Scraper & MongoDB Uploader

A Python toolset for scraping LeetCode question metadata and solutions, then uploading them to MongoDB. This repository helps you build a comprehensive database of LeetCode problems and solutions.

## Features

- 🔍 **Scrape LeetCode Questions**: Automatically fetch question metadata including titles, descriptions, hints, topics, and more
- 💾 **MongoDB Integration**: Store questions and solutions in MongoDB with duplicate checking
- 🚀 **Batch Processing**: Efficient batch uploading for large datasets
- 🔒 **Secure Configuration**: Environment variables for sensitive credentials
- 📊 **Progress Tracking**: Real-time progress bars and detailed statistics

## Prerequisites

- Python 3.7+
- MongoDB Atlas account (or local MongoDB instance)
- LeetCode account (for accessing premium questions, if applicable)

## MongoDB Setup

Before running the scripts, you need to set up your MongoDB database:

1. **Create a MongoDB Atlas account**
   - Go to [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
   - Sign up for a free account (or use an existing account)
   - Create a new cluster (free tier is sufficient)

2. **Create a database and collections**
   - Once your cluster is ready, click "Connect" and then "Browse Collections"
   - Create a new database named `leetcode_questions`
   - The scripts will automatically create the following collections:
     - `question_metadata` - Stores question metadata (created by `uploadMetadata.py`)
     - `python_solutions` - Stores Python solutions (created by `uploadSolutions.py`)
     - `java_solutions` - Stores Java solutions (created by `uploadSolutions.py`)
     - `cpp_solutions` - Stores C++ solutions (created by `uploadSolutions.py`)
   
   Note: Collections are created automatically when you run the scripts, but you can also create them manually if preferred.

3. **Get your connection string**
   - In MongoDB Atlas, click "Connect" on your cluster
   - Choose "Connect your application"
   - Copy the connection string (it will look like: `mongodb+srv://username:password@cluster.mongodb.net/`)
   - Replace `<password>` with your database user password
   - Add `?appName=Cluster0` or similar to the end if needed

4. **Configure network access**
   - In MongoDB Atlas, go to "Network Access"
   - Add your IP address (or `0.0.0.0/0` for development, though not recommended for production)
   - This allows your scripts to connect to the database

## Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd testLeetscrape
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

   Required packages:
   - `pymongo` - MongoDB driver
   - `certifi` - SSL certificates for MongoDB connection
   - `python-dotenv` - Environment variable management
   - `leetscrape` - LeetCode scraping library
   - `beautifulsoup4` - HTML parsing
   - `requests` - HTTP requests
   - `tqdm` - Progress bars

3. **Set up environment variables**
   - Copy `.env.example` to `.env`
   ```bash
   cp .env.example .env
   ```
   - Edit `.env` and add your MongoDB connection string:
   ```env
   MONGODB_URL=your_mongodb_connection_string_here
   ```

## Project Structure

```
testLeetscrape/
├── uploadMetadata.py    # Upload question metadata to MongoDB
├── uploadSolutions.py   # Upload solution code to MongoDB
├── utils.py             # Utility functions for scraping and data processing
├── random.py            # Utility/testing script
├── .env                 # Environment variables (not in git)
├── .env.example         # Environment variable template
├── .gitignore           # Git ignore rules
└── README.md            # This file
```

## File Descriptions

### `uploadMetadata.py`
Main script for uploading LeetCode question metadata to MongoDB. This script:
- Fetches all LeetCode question slugs from the LeetCode API
- Scrapes question metadata (title, description, difficulty, hints, topics, etc.)
- Creates a unique index on `qid` to prevent duplicates
- Performs batch uploading (100 questions per batch) for efficiency
- Skips questions that already exist in the database
- Provides detailed progress tracking and statistics

**Usage:**
```bash
python uploadMetadata.py
```

**Database Collection:** `leetcode_questions.question_metadata`

**Document Structure:**
- `qid`: Question ID (unique)
- `title`: Question title
- `slug`: Question slug
- `difficulty`: Difficulty level (Easy/Medium/Hard)
- `hints`: Array of hints
- `companies`: Associated companies
- `topics`: Problem topics/tags
- `similar_questions`: Similar question IDs
- `code_stub`: Code template
- `question_body`: Cleaned question text
- `is_premium_question`: Boolean flag

### `uploadSolutions.py`
Script for uploading LeetCode solution code to MongoDB. This script:
- Scans a local directory containing solution files
- Supports multiple languages (Python, Java, C++)
- Organizes solutions by question ID and language
- Creates unique indexes on `qid` for each language collection
- Performs batch uploading (200 solutions per batch)
- Skips duplicate solutions efficiently

**Usage:**
```bash
python uploadSolutions.py
```

**Requirements:**
- Solutions should be organized in directories named `{qid}. {title}`
- Solution files should be named `{filename}.{ext}` where `ext` is `py`, `java`, or `cpp`
- Update the `directory` variable in the script to point to your solutions folder

**Database Collections:**
- `leetcode_questions.python_solutions`
- `leetcode_questions.java_solutions`
- `leetcode_questions.cpp_solutions`

**Document Structure:**
- `qid`: Question ID
- `code`: Array of solution code strings
- `language`: Programming language

### `utils.py`
Utility functions used across the project:
- `getSlugs()`: Fetches all LeetCode question slugs from the API
- `clean_text(html_text)`: Converts HTML content to plain text, handling superscripts and formatting
- `getJsonObjFromQuestion(slug)`: Scrapes a LeetCode question by slug and returns a structured JSON object

This file uses the [LeetScrape](https://nikhil-ravi.github.io/LeetScrape/) library for scraping LeetCode questions. Check out the [LeetScrape API documentation](https://nikhil-ravi.github.io/LeetScrape/) to understand the available methods and options.

### `random.py`
Utility/testing script for scanning and processing solution directories. Currently contains helper code for directory traversal and file processing.

## Configuration

### Environment Variables

Create a `.env` file in the root directory with the following:

```env
MONGODB_URL=mongodb+srv://username:password@cluster.mongodb.net/?appName=Cluster0
```

**Note:** Never commit your `.env` file to git! It's already in `.gitignore`.

### MongoDB Database Structure

The scripts use the following MongoDB structure:

- **Database:** `leetcode_questions`
  - **Collections:**
    - `question_metadata` - Question metadata
    - `python_solutions` - Python solutions
    - `java_solutions` - Java solutions
    - `cpp_solutions` - C++ solutions

All collections have a unique index on `qid` to prevent duplicate entries.

## Usage Examples

### Upload All Question Metadata

```bash
python uploadMetadata.py
```

This will:
1. Fetch all question slugs from LeetCode
2. Check existing questions in MongoDB
3. Scrape and upload missing questions in batches
4. Display progress and final statistics

### Upload Solutions

For this project, I copied the [walkccc/LeetCode](https://github.com/walkccc/LeetCode/tree/main) repository to my local computer and uploaded the solutions from there. However, there are probably better ways to handle solution uploads (e.g., using Git submodules, downloading directly from the repository, or organizing your own solutions).

If you want to use a similar approach:

1. **Get solution files**
   - Clone or download the [walkccc/LeetCode repository](https://github.com/walkccc/LeetCode/tree/main)
   - Or organize your own solutions in a directory structure:
     ```
     solutions/
     ├── 1. Two Sum/
     │   ├── solution.py
     │   └── solution.java
     ├── 2. Add Two Numbers/
     │   └── solution.py
     └── ...
     ```

2. **Update the directory path**
   - Edit `uploadSolutions.py` and update the `directory` variable:
     ```python
     directory = "/path/to/your/solutions"
     ```

3. **Run the script**
   ```bash
   python uploadSolutions.py
   ```

## Features & Best Practices

### Duplicate Prevention
- All scripts check for existing entries before uploading
- Unique indexes ensure data integrity at the database level
- Efficient set-based duplicate checking for faster processing

### Batch Processing
- Metadata: 100 questions per batch
- Solutions: 200 solutions per batch
- Automatically flushes remaining items at the end

### Error Handling
- Graceful handling of duplicate errors
- Continues processing even if individual items fail
- Detailed error messages for troubleshooting

### Security
- Environment variables for sensitive credentials
- `.env` file excluded from version control
- `.env.example` provided as a template

## Troubleshooting

### Connection Issues
- Verify your MongoDB connection string in `.env`
- Check if your IP is whitelisted in MongoDB Atlas
- Ensure `certifi` is installed for SSL connections

### Scraping Errors
- Some questions may fail due to network issues or rate limiting
- The script will continue processing other questions
- Check error messages for specific question IDs that failed

### Duplicate Errors
- If you see duplicate errors, it means the question already exists
- The script automatically skips duplicates during batch uploads
- Use the existing qid checking to skip before processing

## Resources

- **[LeetScrape API Documentation](https://nikhil-ravi.github.io/LeetScrape/)** - Check out the LeetScrape library documentation to understand the scraping API and available methods
- **[walkccc/LeetCode Repository](https://github.com/walkccc/LeetCode/tree/main)** - Repository containing LeetCode solutions in multiple languages that I used for this project

## Notes

- The LeetCode scraping relies on the `leetscrape` library. See the [LeetScrape documentation](https://nikhil-ravi.github.io/LeetScrape/) for more details on the API
- Rate limiting may apply when scraping many questions
- Premium questions require a LeetCode Premium subscription
- Make sure to respect LeetCode's terms of service when scraping
- For solutions, I copied the walkccc/LeetCode repository locally and uploaded from there, but there are probably better ways to integrate solution sources

# leetcode_database
