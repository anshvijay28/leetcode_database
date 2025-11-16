from leetscrape import GetQuestion
from bs4 import BeautifulSoup
import requests
import re
from typing import Optional, Tuple


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