import json
import argparse

from utils import invoke, evaluate_time_predictions
from threadpool import ThreadPool
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument(
    "--prompt_file", type=str, default="prompts/v0.txt", help="prompt file"
)
parser.add_argument(
    "--bench_file", type=str, default="../eval_ppl/dataset/abs_time_queries.json", help="benchmark dataset file"
)
args = parser.parse_args()

with open(args.prompt_file) as f:
    prompt = f.read()

with open(args.bench_file) as f:
    time_bench = json.load(f)

# Function to process a single example
def process_example(example, prompt):
    params = {
        "question": example["question"],
        "current_time_iso": example["now"].replace("TIMESTAMP('", "").replace("')", ""),
        "date_field": example["date_field"],
        "other_date_fields": example["other_date_fields"]
    }
    
    return invoke(prompt, **params)


# Create a thread pool
thread_pool = ThreadPool(max_workers=2)

# Use the thread pool to process all examples in parallel
predicts = thread_pool.map_with_args(
    func=process_example,
    items=time_bench[:1],
    fixed_args={"prompt": prompt},
    desc="Processing examples",
)

print(predicts)