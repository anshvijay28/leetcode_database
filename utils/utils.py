from leetscrape import GetQuestion
from bs4 import BeautifulSoup
import requests
import re
from typing import Optional, Tuple, List
from PIL import Image
import io


def getSlugs():
    req = requests.get("https://leetcode.com/api/problems/all/")
    body = req.json()

    # store question number and title slug
    questions = []

    for question in body["stat_status_pairs"]:
        qNum = (
            question["stat"]["frontend_question_id"]
            if "frontend_question_id" in question["stat"]
            else ""
        )
        titleSlug = (
            question["stat"]["question__title_slug"]
            if "question__title_slug" in question["stat"]
            else ""
        )

        questions.append([qNum, titleSlug])

    questions.sort(key=lambda x: x[0])
    return [x[1] for x in questions]


# GOAL: get rid of all substrings beginning and ending with "<" and ">"
def clean_text(html_text: str) -> str:
    # First, handle <sup> tags by converting them to "^" notation
    # e.g., 10<sup>4</sup> -> 10^4

    soup = BeautifulSoup(html_text, "html.parser")

    for sup in soup.find_all("sup"):
        sup.replace_with(f"^{sup.get_text()}")

    # Replace <code> and <em>/<strong> tags with their text content (remove HTML)
    for tag in soup.find_all(["code", "em", "strong"]):
        tag.replace_with(tag.get_text())

    # Convert the rest of HTML to plain text
    clean_text = soup.get_text()

    return clean_text


def getJsonObjFromQuestion(slug):
    question = GetQuestion(titleSlug=slug).scrape()

    # format hints
    hints = []
    for hint in question.Hints:
        hints.append(clean_text(hint))

    # format question
    question_body = clean_text(question.Body)

    return {
        "qid": question.QID,
        "title": question.title,
        "slug": slug,
        "difficulty": question.difficulty,
        "hints": hints,
        "companies": question.Companies,
        "topics": question.topics,
        "similar_questions": question.SimilarQuestions,
        "code_stub": question.Code,
        "question_body": question_body,
        "is_premium_question": question.isPaidOnly,
    }


def extract_constraints_and_followup(question_body: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Extract constraints and follow-up sections from a LeetCode question body.
    
    Args:
        question_body: The full question body text
        
    Returns:
        A tuple of (main_question, constraints, follow_up) where:
        - main_question: The question text without constraints and follow-up
        - constraints: The constraints section (None if not found)
        - follow_up: The follow-up section (None if not found)
    """
    if not question_body:
        return question_body, None, None
    
    constraints = None
    follow_up = None
    main_question = question_body
    
    # Find the positions of Constraints and Follow-up sections
    # Handle both regular spaces and non-breaking spaces (\xa0)
    constraints_start = re.search(r'Constraints:?[\s\xa0]*\n?', question_body, re.IGNORECASE)
    follow_up_start = re.search(r'Follow-up:[\s\xa0]*', question_body, re.IGNORECASE)
    
    if constraints_start:
        # Extract everything from Constraints onwards
        constraints_and_after = question_body[constraints_start.end():]
        
        if follow_up_start and follow_up_start.start() > constraints_start.start():
            # Follow-up exists after Constraints
            # Extract constraints (everything before Follow-up)
            constraints = constraints_and_after[:follow_up_start.start() - constraints_start.end()].strip()
            
            # Extract follow-up (everything after "Follow-up:")
            follow_up_text = question_body[follow_up_start.end():].strip()
            follow_up = follow_up_text
            
            # Remove both Constraints and Follow-up from main question
            main_question = question_body[:constraints_start.start()].strip()
        else:
            # No Follow-up, just Constraints
            constraints = constraints_and_after.strip()
            # Remove Constraints from main question
            main_question = question_body[:constraints_start.start()].strip()
    elif follow_up_start:
        # Only Follow-up exists (no Constraints)
        follow_up = question_body[follow_up_start.end():].strip()
        # Remove Follow-up from main question
        main_question = question_body[:follow_up_start.start()].strip()
    
    return main_question, constraints, follow_up


def split_image_into_tiles(image_path: str, max_dimension: int = 8000, overlap: int = 200) -> List[Tuple[bytes, str]]:
    """
    Split a large image into tiles that fit within max_dimension.
    Returns image data (bytes) for each tile, not file paths.
    
    Args:
        image_path: Path to the image file
        max_dimension: Maximum allowed dimension (default: 8000 for Claude API)
        overlap: Overlap between tiles in pixels to avoid cutting text (default: 200)
    
    Returns:
        List of (image_bytes, media_type) tuples for each tile
    """
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            image_format = img.format or "PNG"
            
            # Determine media type
            media_type_map = {
                "PNG": "image/png",
                "JPEG": "image/jpeg",
                "GIF": "image/gif",
                "WEBP": "image/webp"
            }
            media_type = media_type_map.get(image_format, "image/png")
            
            # Check if splitting is needed
            if width <= max_dimension and height <= max_dimension:
                # No splitting needed, return original image as bytes
                with open(image_path, "rb") as f:
                    return [(f.read(), media_type)]
            
            # Calculate step size (accounting for overlap)
            # Each tile should be max_dimension, but we step by (max_dimension - overlap) to create overlap
            step_size = max_dimension - overlap
            
            tiles = []
            
            # Split image into tiles
            for y in range(0, height, step_size):
                for x in range(0, width, step_size):
                    # Calculate tile boundaries
                    # For first tile, start at 0; for others, include overlap
                    left = max(0, x - overlap if x > 0 else 0)
                    top = max(0, y - overlap if y > 0 else 0)
                    # Each tile is max_dimension wide/tall (or remaining size)
                    right = min(width, left + max_dimension)
                    bottom = min(height, top + max_dimension)
                    
                    # Crop the tile
                    tile = img.crop((left, top, right, bottom))
                    tile_width, tile_height = tile.size
                    
                    # Verify tile dimensions are within limit
                    if tile_width > max_dimension or tile_height > max_dimension:
                        print(f"  ⚠️  Warning: Tile {len(tiles) + 1} is {tile_width}x{tile_height}, exceeding {max_dimension}")
                    
                    # Convert to bytes
                    img_bytes = io.BytesIO()
                    tile.save(img_bytes, format=image_format)
                    img_bytes.seek(0)
                    
                    tiles.append((img_bytes.read(), media_type))
            
            print(f"  📐 Split image {width}x{height} into {len(tiles)} tile(s)")
            return tiles
            
    except Exception as e:
        print(f"  ⚠️  Error splitting image: {e}")
        import traceback
        traceback.print_exc()
        raise