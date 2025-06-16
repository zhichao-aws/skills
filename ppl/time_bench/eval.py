import json
import argparse
import os
import re

from utils import invoke
from threadpool import ThreadPool

parser = argparse.ArgumentParser()
parser.add_argument(
    "--prompt_file", type=str, default="prompts/v4.txt", help="prompt file"
)
parser.add_argument(
    "--bench_root",
    type=str,
    default="../eval_ppl/dataset",
    help="Root directory for benchmark files",
)
parser.add_argument(
    "--bench_file", type=str, default="abs_time_queries.json", help="Benchmark filename"
)
parser.add_argument(
    "--output_root",
    type=str,
    default="../eval_ppl/dataset_parsed",
    help="Root directory for output files",
)
args = parser.parse_args()

with open(args.prompt_file) as f:
    prompt = f.read()

# Construct full paths
bench_path = os.path.join(args.bench_root, args.bench_file)
output_path = os.path.join(args.output_root, args.bench_file)

# Ensure output directory exists
os.makedirs(args.output_root, exist_ok=True)

with open(bench_path) as f:
    time_bench = json.load(f)


def normalize_date_format(date_string):
    # Extract date part (YYYY-MM-DD)
    date_pattern = r"(\d{4}-\d{2}-\d{2})"
    date_match = re.search(date_pattern, date_string)
    # Extract time part (HH:MM:SS)
    time_pattern = r"(\d{2}:\d{2}:\d{2})"
    time_match = re.search(time_pattern, date_string)

    # Check if date and time were successfully extracted
    if not date_match or not time_match:
        return None

    # Get matched date and time
    date_part = date_match.group(1)
    time_part = time_match.group(1)

    # Return standardized format
    return f"{date_part} {time_part}"


# Function to process a single example
def process_example(example, prompt):
    params = {
        "question": example["question"],
        "current_time_iso": example["now"].replace("TIMESTAMP('", "").replace("')", ""),
        "date_field": example["date_field"],
        "other_date_fields": example["other_date_fields"],
    }

    res = invoke(prompt, **params)
    res["question"] = example["question"]
    for key in ["date_field", "now", "target_index"]:
        res[key] = example[key]

    for key in ["start", "end"]:
        if res[key] is not None:
            res[key] = normalize_date_format(res[key])
    return res


# Create a thread pool
thread_pool = ThreadPool(max_workers=2)

# Use the thread pool to process all examples in parallel
predicts = thread_pool.map_with_args(
    func=process_example,
    items=time_bench,
    fixed_args={"prompt": prompt},
    desc="Processing examples",
)

with open(output_path, "w") as f:
    json.dump(predicts, f, indent=4)

for i in range(len(time_bench)):
    if time_bench[i]["time_range"][0]!= predicts[i]["start"] or time_bench[i]["time_range"][1]!= predicts[i]["end"]:
        print(i, time_bench[i]["question"], time_bench[i]["time_range"], (predicts[i]["start"], predicts[i]["end"]))