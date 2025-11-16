"""
LeetCode Problem Summary Generation Prompt

This module contains the prompt template for generating comprehensive,
searchable summaries of LeetCode problems using an LLM.

Usage:
    from summary_prompt import SUMMARY_GENERATION_PROMPT
    
    # Combine with your question data
    full_prompt = SUMMARY_GENERATION_PROMPT + question_data
    
    # Send to LLM API
    response = llm_api.complete(full_prompt)
"""

SUMMARY_GENERATION_PROMPT = """You are an expert algorithm analyst creating comprehensive, searchable summaries for a LeetCode problem database. Your summary will be embedded and used for semantic search, so it must capture all aspects that users might query in natural language.

A complete problem specification follows this prompt, including the problem statement, constraints, hints, and solution code in multiple languages. Your task is to analyze ALL provided information and generate a detailed, cohesive summary that covers multiple dimensions of this problem.

ANALYSIS REQUIREMENTS:
- Examine the actual solution code provided to determine time and space complexity
- Identify the data structures by reading the code implementations (don't assume)
- If multiple solutions are provided in different languages, analyze the approach (they typically use the same algorithm)
- Use the hints to understand the problem-solving progression
- Extract insights from the code comments and variable names
- Identify the core algorithmic pattern by analyzing the code structure

SUMMARY STRUCTURE:

Write your summary in clear, technical prose using complete sentences and paragraphs (not bullet points). Use the following organization:

**1. Problem Essence**
In 2-3 sentences, clearly state what the problem asks for, the input/output format, and the core challenge. Avoid just repeating the title—explain what you're actually trying to accomplish and what makes it non-trivial.

**2. Data Structures Used**
List and briefly explain ALL data structures employed in the solution(s). For each structure, mention WHY it's used:
- Built-in structures (arrays, hashmaps, sets, queues, stacks, heaps, strings)
- Custom structures (graphs as adjacency lists, trees, tries, union-find, segments trees)
- Implicit structures (2D matrix treated as graph, array as implicit tree, memoization table)

Examples of good explanations:
- "A 2D DP table where dp[i][j] represents whether substring s[0..i) matches pattern p[0..j)"
- "HashMap for O(1) lookup of previously seen values and their indices"
- "Queue for BFS traversal to explore nodes level by level"

**3. Algorithms & Techniques Applied**
Identify ALL algorithmic approaches used by analyzing the code structure:
- Search algorithms (BFS, DFS, binary search, backtracking)
- Optimization techniques (dynamic programming, greedy, divide and conquer)
- Processing patterns (sliding window, two pointers, prefix sums, monotonic stack)
- Graph algorithms (Dijkstra, topological sort, union-find, cycle detection)
- String algorithms (KMP, rolling hash, trie-based matching)
- Mathematical techniques (bit manipulation, modular arithmetic, combinatorics)

If multiple techniques are combined, explicitly state this (e.g., "Binary search on the answer space, with each validation using BFS on a 2D matrix" or "Dynamic programming with memoization combined with recursion").

**4. Core Insight & Intuition**
What is the key realization that unlocks this problem? This should be the "aha moment" that transforms it from impossible to solvable. Look at the hints provided and the structure of the solution to identify this.

Examples of core insights:
- "The key insight is recognizing this as a monotonicity property that enables binary search on the answer"
- "The breakthrough is seeing that we can build solutions incrementally using previously computed subproblems"
- "The trick is realizing that '*' can match zero or more of the preceding character, requiring us to consider both taking and skipping options"
- "The critical observation is that we can maintain an invariant where the sliding window always contains valid elements"

**5. Solution Strategy & Complexity Analysis**
First, analyze the provided solution code to determine:
- **Time Complexity: O(?)** - explain why based on the loops, recursion depth, and operations in the code
- **Space Complexity: O(?)** - explain why based on data structures used (arrays, recursion stack, memoization tables)

Then describe the high-level approach in 4-6 sentences. Explain the flow of the algorithm without getting into implementation details. If the hints suggest alternative approaches (like brute force), briefly mention how this solution improves upon them.

**6. Pattern Classification**
Identify the canonical patterns and problem archetypes this problem exemplifies:
- Named patterns (e.g., "Fast & Slow Pointers", "Merge Intervals", "Topological Sort", "0/1 Knapsack", "Longest Common Subsequence")
- Problem category (optimization, counting, pathfinding, simulation, constructive, game theory, parsing, matching)
- Template family (e.g., "This follows the 2D DP template", "This is a standard backtracking with pruning problem")

Mention if this is a disguised problem (e.g., "Appears as a string problem but is actually a graph problem requiring cycle detection" or "Looks like a greedy problem but actually requires DP due to optimal substructure").

**7. Difficulty Analysis**
Explain what makes this problem its rated difficulty:
- **For Easy:** What makes it straightforward? What single concept does it test?
- **For Medium:** What non-obvious technique or combination is needed? What makes it harder than Easy?
- **For Hard:** What multiple insights, complex implementation, or subtle edge cases are required?

List prerequisite concepts you should know before attempting this problem (e.g., "Understanding of basic DP", "Familiarity with graph traversal", "Knowledge of string manipulation").

**8. Constraints & Their Implications**
Analyze how the input constraints guide the solution approach:
- Small N (≤100) allows O(N³) solutions
- Large N (≤10⁵) requires O(N log N) or better
- Value ranges that suggest certain approaches
- Guarantees that eliminate edge cases

Examples:
- "N ≤ 20 allows O(2^N) exponential solutions like backtracking or bitmask DP"
- "Values are in range [1, 10⁹] requiring binary search or hashing, not array indexing"
- "Guarantee of exactly one solution means we can return immediately upon finding a match"

Mention critical edge cases to consider: empty inputs, single elements, duplicates, negative numbers, cycles, disconnected components, overlapping intervals, etc.

**9. Common Pitfalls & Mistakes**
What do people typically get wrong when solving this problem?
- Off-by-one errors in specific places (array indexing, DP table dimensions)
- Forgetting to handle certain edge cases
- Incorrect ordering of operations
- Wrong data structure choice leading to TLE or WA
- Misunderstanding problem requirements

**10. Related Concepts & Connections**
- Mathematical foundations (if applicable): graph theory, number theory, probability, geometry, automata theory
- Real-world analogies that make the problem intuitive
- What this problem is conceptually similar to
- Interview context: "Common in system design discussions", "Tests understanding of space-time tradeoffs", "Popular at FAANG companies"

**11. Natural Language Search Terms**
Write a paragraph that includes alternative phrasings and synonyms someone might use to search for this problem. Think about how beginners vs experts might describe it differently. Include:
- Technical terms and colloquialisms
- Different ways to describe the same concept
- Related but not identical problem types
- Both abstract and concrete descriptions

Examples: "pattern matching", "string matching with wildcards", "regex implementation", "DP on strings", "character matching with special symbols", "wildcard matching problem", "text pattern validation", "string comparison with dots and stars"

WRITING GUIDELINES:
- Be specific with algorithm/data structure names (don't just say "searching", say "binary search" or "BFS" or "dynamic programming")
- Explain WHY techniques work, not just WHAT they are
- Include both obvious and subtle characteristics
- Use terminology that spans different skill levels (both "DP" and "dynamic programming", both "memo table" and "memoization array")
- Make connections between concepts (e.g., "This uses DP, but unlike typical DP on arrays, we're building a 2D table to capture relationships between two sequences")
- Write in complete sentences and paragraphs, not bullet points
- Aim for 700-900 words total for a comprehensive summary
- Use precise technical terminology while remaining searchable

Generate the complete summary now based on the problem information that follows:

"""


def get_prompt() -> str:
    """
    Returns the summary generation prompt.
    
    Returns:
        str: The complete prompt template for LLM summary generation
    """
    return SUMMARY_GENERATION_PROMPT


def create_full_prompt(question_data: str) -> str:
    """
    Combines the prompt template with question data.
    
    Args:
        question_data: The formatted question data including title, difficulty,
                      problem statement, constraints, and solution code
    
    Returns:
        str: Complete prompt ready to send to LLM API
    
    Example:
        >>> question = load_question_from_db(question_id)
        >>> full_prompt = create_full_prompt(question)
        >>> summary = llm_api.complete(full_prompt)
    """
    return f"{SUMMARY_GENERATION_PROMPT}\n\n=== PROBLEM DATA ===\n{question_data}"
