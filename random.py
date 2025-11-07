import os
import re
from tqdm import tqdm

# def get_problem_numbers(directory):
#     problem_numbers = []
#     for name in os.listdir(directory):
#         # Match directories that start with a number followed by a dot and space
#         match = re.match(r"^(\d+)\.", name)
#         if match:
#             problem_numbers.append(int(match.group(1)))
#     return problem_numbers

# if __name__ == "__main__":
#     directory = "/Users/anshvijay/Desktop/LeetCode/solutions"  # change this to your directory path
#     problems = get_problem_numbers(directory)
#     problems = sorted(problems)
#     missing = []
#     for i in range(len(problems) - 1):
#         if problems[i+1] - problems[i] != 1:
#             print(f"Missing problem {problems[i] + 1}")
#             missing.append(problems[i] + 1)
    
#     # 1 - 3563
#     print(len(problems))

directory = "/Users/anshvijay/Desktop/LeetCode/solutions"
for name in tqdm(os.listdir(directory), desc="Scanning directories"):

    # Match directories that start with a number followed by a dot and space
    match = re.match(r"^(\d+)\.", name)
    if not match:
        continue

    subdirectory_path = os.path.join(directory, name)

    if not os.path.isdir(subdirectory_path):
        continue
    files_in_subdirectory = os.listdir(subdirectory_path)
    qid = int(name.split(".")[0])

    print(qid)

    # for file_name in files_in_subdirectory:
        
    #     # 1) Parts check
    #     parts = file_name.split(".")
    #     if len(parts) < 2:
    #         continue
        
    #     # 2) Language check
    #     qid = parts[0]
    #     language = parts[1]

    #     # 3) qid check, sometimes there can be multiple solutions
    #     qid_parts = qid.split("-")[0]


    #     print(qid, language, parts)