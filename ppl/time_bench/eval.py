import json
import argparse

from utils import invoke, evaluate_time_predictions
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

predicts = []
for example in tqdm(time_bench):
    predicts.append(invoke(example["question"], example["now"], prompt))

eval_res = evaluate_time_predictions(time_bench, predicts, True)

with open(f"prompts/{args.version}_res.json", "w") as f:
    json.dump(eval_res, f, indent=4)

all_res = [(i, time_bench[i], predicts[i]) for i in range(len(time_bench))]
with open(f"prompts/{args.version}_all.json", "w") as f:
    json.dump(all_res, f, indent=4)
