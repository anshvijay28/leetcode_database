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

SUMMARY_GENERATION_PROMPT = """You are an expert algorithm analyst creating comprehensive, searchable summaries for a LeetCode problem database. Your summary will be embedded and used for semantic search in a retrieval-augmented generation (RAG) system, where users query for LISTS of relevant problems rather than individual problems.

<task_context>
Users will search using natural language queries like "show me dynamic programming problems on arrays" or "find graph problems requiring BFS." Your summary must ensure this problem appears in search results when it's relevant to the query. The better your summary captures multiple facets of the problem, the more discoverable it becomes.
</task_context>

<input_specification>
A complete problem specification follows this prompt, including:
- Problem statement with examples
- Constraints and hints
- Solution code in multiple languages (Python, Java, C++)

Your task: Analyze ALL provided information and generate a detailed, cohesive summary covering multiple dimensions of this problem.
</input_specification>

<analysis_workflow>
Before writing your summary, you must:

1. **Code Analysis**: Examine ALL solution implementations across all languages
   - Determine time and space complexity for each approach
   - Identify data structures by reading actual code (never assume)
   - CRITICAL: Multiple solutions may use DIFFERENT algorithmic approaches, not just the same algorithm in different languages
   - If you find distinct techniques (iterative DP vs recursive memoization, BFS vs DFS, HashMap vs sorting), document ALL of them

2. **Pattern Recognition**: From code structure and hints
   - Identify core algorithmic patterns
   - Recognize problem-solving progressions suggested by hints
   - Extract insights from code comments and variable names

3. **Characteristic Assessment**: Classify the problem
   - Implementation complexity (code-heavy vs concept-heavy)
   - Intuitiveness (requires "aha moment" vs straightforward)
   - Mathematical reasoning requirements
</analysis_workflow>

<summary_format>
Write your summary in clear, technical prose using complete sentences and paragraphs. Use the following structure:

<section_1_problem_essence>
**1. Problem Essence**
In 2-3 sentences, clearly state what the problem asks for, the input/output format, and the core challenge. Explain what you're actually trying to accomplish and what makes it non-trivial—avoid just repeating the title.
</section_1_problem_essence>

<section_2_data_structures>
**2. Data Structures Used**
List and briefly explain ALL data structures employed across ALL solutions. For each structure, explain WHY it's used:
- Built-in structures (arrays, hashmaps, sets, queues, stacks, heaps, strings)
- Custom structures (graphs as adjacency lists, trees, tries, union-find, segment trees)
- Implicit structures (2D matrix treated as graph, array as implicit tree, memoization table)

If multiple solutions use different data structures, specify which structures belong to which approach (e.g., "The DP approach uses a 2D array, while the HashMap approach uses a hash table for O(1) lookups").

Example explanations:
- "A 2D DP table where dp[i][j] represents whether substring s[0..i) matches pattern p[0..j)"
- "HashMap for O(1) lookup of previously seen values and their indices"
- "Queue for BFS traversal to explore nodes level by level"
</section_2_data_structures>

<section_3_algorithms>
**3. Algorithms & Techniques Applied**
CRITICAL FOR MULTI-APPROACH PROBLEMS: If multiple solutions are provided, they may use DIFFERENT algorithms. Identify and describe ALL algorithmic approaches found:
- Search algorithms (BFS, DFS, binary search, backtracking)
- Optimization techniques (dynamic programming, greedy, divide and conquer)
- Processing patterns (sliding window, two pointers, prefix sums, monotonic stack)
- Graph algorithms (Dijkstra, topological sort, union-find, cycle detection)
- String algorithms (KMP, rolling hash, trie-based matching)
- Mathematical techniques (bit manipulation, modular arithmetic, combinatorics)

For each distinct approach, briefly explain the technique. If multiple techniques combine in a single solution, state this explicitly (e.g., "Binary search on the answer space, with each validation using BFS on a 2D matrix").

If different solutions use different approaches, clearly distinguish them (e.g., "Solution 1 uses iterative DP with tabulation, while Solution 2 uses recursive DFS with memoization, and Solution 3 uses a greedy approach with sorting").
</section_3_algorithms>

<section_4_core_insight>
**4. Core Insight & Intuition**
What is the key realization that unlocks this problem? Identify the "aha moment" that transforms it from impossible to solvable. Use the hints and solution structure to identify this.

Example insights:
- "The key insight is recognizing this as a monotonicity property that enables binary search on the answer"
- "The breakthrough is seeing that we can build solutions incrementally using previously computed subproblems"
- "The trick is realizing that '*' can match zero or more of the preceding character, requiring us to consider both taking and skipping options"
- "The critical observation is that we can maintain an invariant where the sliding window always contains valid elements"
</section_4_core_insight>

<section_5_complexity_strategy>
**5. Solution Strategy & Complexity Analysis**
For EACH distinct solution approach provided:
- **Time Complexity: O(?)** - explain why based on loops, recursion depth, and operations
- **Space Complexity: O(?)** - explain why based on data structures used (arrays, recursion stack, memoization tables)

If multiple solutions with different approaches exist, analyze each separately (e.g., "Approach 1 (Brute Force): O(n²) time, O(1) space. Approach 2 (Hash Map): O(n) time, O(n) space").

Then describe the high-level strategy for each approach in 2-4 sentences. Explain the algorithm flow without implementation details. If hints suggest alternative approaches or multiple solutions demonstrate different trade-offs, discuss the progression from brute force to optimal.
</section_5_complexity_strategy>

<section_6_patterns>
**6. Pattern Classification**
Identify ALL canonical patterns and problem archetypes across all solutions:
- Named patterns (e.g., "Fast & Slow Pointers", "Merge Intervals", "Topological Sort", "0/1 Knapsack", "Longest Common Subsequence")
- Problem category (optimization, counting, pathfinding, simulation, constructive, game theory, parsing, matching)
- Template family (e.g., "This follows the 2D DP template", "This is a standard backtracking with pruning problem")

If multiple solutions use different patterns, list all of them (e.g., "Can be solved with either the Sliding Window pattern or the Two Pointers pattern").

Mention if this is a disguised problem (e.g., "Appears as a string problem but is actually a graph problem requiring cycle detection" or "Looks like a greedy problem but actually requires DP due to optimal substructure").
</section_6_patterns>

<section_7_difficulty>
**7. Difficulty Analysis**
Explain what makes this problem its rated difficulty:
- **For Easy:** What makes it straightforward? What single concept does it test?
- **For Medium:** What non-obvious technique or combination is needed? What makes it harder than Easy?
- **For Hard:** What multiple insights, complex implementation, or subtle edge cases are required?

List prerequisite concepts needed before attempting this problem (e.g., "Understanding of basic DP", "Familiarity with graph traversal", "Knowledge of string manipulation").
</section_7_difficulty>

<section_8_characteristics>
**8. Problem Characteristics (Implementation & Cognitive Complexity)**
CRITICAL FOR RAG SEARCH: Classify the problem along these dimensions to enable characteristic-based queries:

**Implementation Complexity:**
- **Implementation-Heavy:** Requires extensive code, careful handling of multiple cases, tedious bookkeeping, or complex data structure manipulation (e.g., simulation problems, parsing problems, problems with many edge cases)
- **Implementation-Light:** Code is relatively short once you know the approach

**Intuition & Insight:**
- **Unintuitive/Tricky:** Requires a non-obvious insight, clever observation, or "aha moment" that's hard to discover (problems where the solution feels like a "trick")
- **Straightforward:** Solution approach is relatively obvious once you understand the problem

**Mathematical Reasoning:**
- **Math-Heavy:** Requires significant mathematical knowledge, proofs, number theory, combinatorics, geometry, or mathematical insights (e.g., problems involving GCD, modular arithmetic, mathematical formulas)
- **Math-Light:** Can be solved with standard algorithmic techniques without deep mathematical reasoning

Provide a 2-3 sentence assessment of where this problem falls on these spectra. This helps users search for problems matching their practice goals (e.g., "looking for implementation-heavy problems" or "problems requiring mathematical insight").
</section_8_characteristics>

<section_9_constraints>
**9. Constraints & Their Implications**
Analyze how input constraints guide the solution approach:
- Small N (≤100) allows O(N³) solutions
- Large N (≤10⁵) requires O(N log N) or better
- Value ranges that suggest certain approaches
- Guarantees that eliminate edge cases

Examples:
- "N ≤ 20 allows O(2^N) exponential solutions like backtracking or bitmask DP"
- "Values are in range [1, 10⁹] requiring binary search or hashing, not array indexing"
- "Guarantee of exactly one solution means we can return immediately upon finding a match"

Mention critical edge cases: empty inputs, single elements, duplicates, negative numbers, cycles, disconnected components, overlapping intervals, etc.
</section_9_constraints>

<section_10_pitfalls>
**10. Common Pitfalls & Mistakes**
What do people typically get wrong when solving this problem?
- Off-by-one errors in specific places (array indexing, DP table dimensions)
- Forgetting to handle certain edge cases
- Incorrect ordering of operations
- Wrong data structure choice leading to TLE or WA
- Misunderstanding problem requirements
</section_10_pitfalls>

<section_11_connections>
**11. Related Concepts & Connections**
- Mathematical foundations (if applicable): graph theory, number theory, probability, geometry, automata theory
- Real-world analogies that make the problem intuitive
- What this problem is conceptually similar to
- Interview context: "Common in system design discussions", "Tests understanding of space-time tradeoffs", "Popular at FAANG companies"
</section_11_connections>

<section_12_search_terms>
**12. Natural Language Search Terms for RAG Discovery**
CRITICAL INSTRUCTION: Users are searching for LISTS of problems, not individual problems. Your search terms should help this problem appear when it's relevant to broader queries like "show me all graph problems using BFS" or "find medium difficulty DP problems."

Write a comprehensive paragraph including alternative phrasings and synonyms someone might use when querying for a list of problems. Include search terms for ALL solution approaches identified. Think about how beginners vs experts might describe problem categories differently:

- Technical terms AND colloquialisms for ALL approaches (e.g., both "dynamic programming" and "DP", both "breadth-first search" and "BFS")
- Category-level descriptions (e.g., "string manipulation problems", "graph traversal questions", "array optimization challenges")
- Pattern-based terms (e.g., "sliding window problems", "two-pointer technique exercises", "prefix sum questions")
- Difficulty-qualified searches (e.g., "medium graph problems", "hard DP questions")
- Characteristic-based terms (e.g., "implementation-heavy array problems", "problems with tricky edge cases", "math-based combinatorics questions")
- Abstract and concrete descriptions (e.g., "problems about finding subsequences" AND "longest common subsequence type problems")
- Hybrid approach terms if multiple solutions exist (e.g., "can be solved with either greedy or DP", "problems solvable with both iterative and recursive approaches")

Examples: "pattern matching problems", "string matching with wildcards", "regex implementation questions", "DP on strings", "character matching with special symbols", "wildcard matching problems", "text pattern validation exercises", "string comparison problems", "problems using recursive backtracking with memoization", "bottom-up tabulation DP questions", "medium difficulty string algorithm problems"
</section_12_search_terms>
</summary_format>

<writing_guidelines>
**Style & Format:**
- Write in clear, technical prose using complete sentences and paragraphs
- Do NOT use bullet points for the main content (the section structure above is for organization only)
- Be specific with algorithm/data structure names (say "binary search" or "BFS" or "dynamic programming", not just "searching")
- Use terminology spanning different skill levels (both "DP" and "dynamic programming", both "memo table" and "memoization array")

**Content Requirements:**
- When multiple solutions exist, describe ALL distinct approaches to maximize searchability
- Explain WHY techniques work, not just WHAT they are
- Include both obvious and subtle characteristics
- Make connections between concepts (e.g., "This uses DP, but unlike typical DP on arrays, we're building a 2D table to capture relationships between two sequences")
- For problems with multiple solutions, ensure users can find the problem regardless of which approach they're thinking of

**Completeness & Thoroughness:**
- Target 800-1000 words total for comprehensive coverage (longer if multiple distinct solutions exist)
- Use precise technical terminology while remaining searchable
- Ensure every section addresses its stated goals completely before moving to the next section
- The search terms section (Section 12) is CRITICAL—spend adequate effort making this problem discoverable through diverse queries

**RAG Optimization:**
Remember: Your summary will be embedded for semantic search. Users query for LISTS of problems like "show me graph problems using DFS" or "medium array problems with two pointers." Your job is to ensure this problem appears in relevant result sets by capturing all its dimensions comprehensively.
</writing_guidelines>

<final_instruction>
Generate the complete summary now based on the problem information that follows. Analyze the code thoroughly, identify all distinct approaches, and create a summary that maximizes this problem's discoverability in a RAG system.
</final_instruction>

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