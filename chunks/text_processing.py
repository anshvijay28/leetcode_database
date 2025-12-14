"""
Text processing module for chunking summaries.

This module handles the core text processing logic for splitting summaries
into chunks based on section headers.
"""

import sys
import re
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from chunks.config import SECTIONS


def chunk_summary(qid: int, summary: str) -> list[dict]:
    """
    Split a problem summary into chunks based on section headers and format for MongoDB.
    
    Each chunk includes the section header and its content.
    Sections are identified using the SECTIONS array from config.py.
    Missing sections are skipped (not included in the returned list).
    Handles various header formats: **text**, ## text, <strong>text</strong>, or plain text.
    
    Args:
        qid: Question ID
        summary: Summary text to chunk
        
    Returns:
        List of MongoDB-ready documents: [{"qid": qid, "chunk_id": chunk_id, "text": text}, ...]
    """
    if not summary:
        return []

    chunks = []
    
    # Build regex patterns for each section that match various formatting styles
    section_patterns = []
    for section in SECTIONS:
        # Extract number and name from section string (e.g., "1. Problem Essence")
        parts = section.split(". ", 1)
        if len(parts) != 2:
            continue
        
        section_num = parts[0]
        section_name = parts[1]
        
        # Escape special regex characters in section name
        escaped_name = re.escape(section_name)
        escaped_num = re.escape(section_num)
        
        # Build pattern that matches:
        # - **{num}. {name}**
        # - ## {num}. {name} or ##{num}. {name}
        # - <strong>{num}. {name}</strong>
        # - {num}. {name} (plain)
        # Pattern allows optional formatting before and after
        pattern = (
            r'(?:\*\*|##\s*|<strong>)?'  # Optional opening format: ** or ## (with optional space) or <strong>
            + escaped_num
            + r'\.\s+'  # Literal dot and whitespace
            + escaped_name
            + r'(?:\*\*|</strong>)?'  # Optional closing format: ** or </strong>
        )
        
        section_patterns.append({
            'pattern': pattern,
            'section': section,
            'num': section_num,
            'name': section_name
        })
    
    # Find all section positions in the summary
    section_matches = []
    for pattern_info in section_patterns:
        pattern = pattern_info['pattern']
        matches = list(re.finditer(pattern, summary))
        for match in matches:
            section_matches.append({
                'start': match.start(),
                'end': match.end(),
                'section': pattern_info['section'],
                'num': pattern_info['num'],
                'name': pattern_info['name'],
                'full_match': match.group(0)
            })
    
    # Sort by position in summary
    section_matches.sort(key=lambda x: x['start'])
    
    # Extract chunks
    for i, match_info in enumerate(section_matches):
        # Normalize header to {num}. {name} format
        normalized_header = f"{match_info['num']}. {match_info['name']}"
        
        # Find content start (after the header)
        content_start = match_info['end']
        
        # Find content end (start of next section or end of summary)
        if i + 1 < len(section_matches):
            content_end = section_matches[i + 1]['start']
        else:
            content_end = len(summary)
        
        # Extract content
        content = summary[content_start:content_end]
        
        # Strip leading/trailing whitespace
        content = content.strip()
        
        # Remove opening section tags (e.g., <section_1_problem_essence>)
        content = re.sub(r'^<section_\w+>\s*', '', content, flags=re.MULTILINE)
        
        # Remove closing section tags (e.g., </section_1_problem_essence>)
        content = re.sub(r'</section_\w+>\s*$', '', content, flags=re.MULTILINE)
        
        # Strip again after removing tags
        content = content.strip()
        
        # Remove leading separator (---) if present (on its own line or at start)
        lines = content.split('\n')
        if lines and lines[0].strip() == '---':
            content = '\n'.join(lines[1:]).strip()
        
        # Remove trailing separator (---) if present (on its own line or at end)
        lines = content.split('\n')
        if lines and lines[-1].strip() == '---':
            content = '\n'.join(lines[:-1]).strip()
        
        # Combine normalized header and content
        if content:  # Only add chunk if there's content
            chunk_text = normalized_header + "\n\n" + content
            # Use section number as chunk_id (convert to int)
            chunk_id = int(match_info['num'])
            # Format as MongoDB document
            chunks.append({
                "qid": qid,
                "chunk_id": chunk_id,
                "text": chunk_text
            })
    
    return chunks
