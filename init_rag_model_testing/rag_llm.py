"""
RAG LLM module for reasoning about questions using OpenAI.

This module handles calling OpenAI's chat API with RAG context.
"""

from typing import List
from openai import AsyncOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = "gpt-5-nano-2025-08-07"  # Using gpt-4o-mini as requested


async def generate_rag_response(query: str, context_documents: List[str], client: AsyncOpenAI) -> str:
    """
    Generate a response using RAG with OpenAI's chat API.
    
    Args:
        query: User query/question
        context_documents: List of formatted context documents
        client: OpenAI async client
    
    Returns:
        Generated response string
    """
    # Combine context documents
    context = "\n\n".join([
        f"--- Document {i+1} ---\n{doc}" 
        for i, doc in enumerate(context_documents)
    ])
    
    # System prompt for RAG
    system_prompt = """You are a helpful assistant that answers questions about LeetCode problems using the provided context documents. 
Analyze the relevant problems from the context and provide a comprehensive answer to the user's question.
If the context contains multiple relevant problems, discuss them all and explain how they relate to the query."""

    # User message with context
    user_message = f"""Context Documents (retrieved via vector search):
{context}

User Query: {query}

Please:
1. Analyze the provided context documents and answer the user's query about LeetCode problems
2. Rank each document by relevance to the query (most relevant first)
3. For each document, provide a detailed justification explaining:
   - Why it is ranked at that position
   - What specific aspects (topics, problem type, difficulty, approach, etc.) make it relevant
   - How well it addresses the user's query

Format your response with:
- Your answer to the query
- A "Document Rankings and Justifications" section listing each document with its ranking and justification"""

    # Call OpenAI API
    response = await client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
    )
    
    return response.choices[0].message.content
