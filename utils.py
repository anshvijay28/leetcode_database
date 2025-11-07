from leetscrape import GetQuestion
from bs4 import BeautifulSoup
import requests


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