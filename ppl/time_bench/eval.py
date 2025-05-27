import json
import argparse

from utils import invoke, evaluate_time_predictions
from threadpool import ThreadPool
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument(
    "--version", type=str, default="v0", help="Version number of prompts"
)
args = parser.parse_args()

with open(f"prompts/{args.version}.txt") as f:
    prompt = f.read()

with open("time_bench.json") as f:
    time_bench = json.load(f)

# Function to process a single example
def process_example(example, prompt):
    return invoke(example["question"], example["now"], prompt)


# Create a thread pool
thread_pool = ThreadPool(max_workers=2)

# Use the thread pool to process all examples in parallel
predicts = thread_pool.map_with_args(
    func=process_example,
    items=time_bench,
    fixed_args={"prompt": prompt},
    desc="Processing examples",
)

eval_res = evaluate_time_predictions(time_bench, predicts, True)

with open(f"prompts/{args.version}_res.json", "w") as f:
    json.dump(eval_res, f, indent=4)

all_res = [(i, time_bench[i], predicts[i]) for i in range(len(time_bench))]
with open(f"prompts/{args.version}_all.json", "w") as f:
    json.dump(all_res, f, indent=4)
