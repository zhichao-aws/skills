import os
import json
import argparse

from utils import eval_execution_accuracy, simple_parse


def compare_results(ground_truth_file, to_eval_file, target_file):
    # Load the files
    with open(ground_truth_file, "r") as f:
        ground_truth = json.load(f)
    with open(to_eval_file, "r") as f:
        to_eval = json.load(f)

    total_count = len(ground_truth)
    syntax_errors = 0
    data_mismatches = 0
    length_mismatches = 0
    error_compare = 0
    matches = 0
    mismatched_queries = []

    # Compare each pair of samples
    for gt_sample, eval_sample in zip(ground_truth, to_eval):
        # Check for syntax errors
        if eval_sample["data_rows"] == "ERROR":
            syntax_errors += 1
            mismatched_queries.append(
                {
                    "now": gt_sample["now"],
                    "ground_truth_query": gt_sample["query"],
                    "eval_query": eval_sample["query"],
                    "error": "Syntax Error",
                }
            )
            continue

        # Get data rows
        gt_rows = gt_sample["data_rows"]
        eval_rows = eval_sample["data_rows"]

        try:
            if eval_execution_accuracy(
                simple_parse(eval_rows, eval_sample["schema"]),
                simple_parse(gt_rows, gt_sample["schema"]),
            ):
                matches += 1
            else:
                if len(gt_rows) == len(eval_rows):
                    mismatched_queries.append(
                        {
                            "question": gt_sample["question"],
                            "target_index": gt_sample["target_index"],
                            "now": gt_sample["now"],
                            "ground_truth_query": gt_sample["query"],
                            "eval_query": eval_sample["query"],
                            "error": "Data Mismatch",
                            "results": [
                                gt_sample["data_rows"][:2],
                                eval_sample["data_rows"][:2],
                            ],
                        }
                    )
                else:
                    mismatched_queries.append(
                        {
                            "question": gt_sample["question"],
                            "target_index": gt_sample["target_index"],
                            "now": gt_sample["now"],
                            "ground_truth_query": gt_sample["query"],
                            "eval_query": eval_sample["query"],
                            "error": "Length Mismatch",
                            "results": [
                                len(gt_sample["data_rows"]),
                                len(eval_sample["data_rows"]),
                            ],
                        }
                    )
        except:
            mismatched_queries.append(
                {
                    "question": gt_sample["question"],
                    "target_index": gt_sample["target_index"],
                    "now": gt_sample["now"],
                    "ground_truth_query": gt_sample["query"],
                    "eval_query": eval_sample["query"],
                    "error": "Error Compare",
                    "results": [
                        gt_sample["data_rows"][:2],
                        eval_sample["data_rows"][:2],
                    ],
                }
            )

    # Count different types of mismatches
    for mismatch in mismatched_queries:
        if mismatch["error"] == "Data Mismatch":
            data_mismatches += 1
        elif mismatch["error"] == "Length Mismatch":
            length_mismatches += 1
        elif mismatch["error"] == "Error Compare":
            error_compare += 1

    # Calculate proportions
    syntax_error_rate = syntax_errors / total_count
    match_rate = matches / total_count
    data_mismatch_rate = data_mismatches / total_count
    length_mismatch_rate = length_mismatches / total_count

    # Prepare results
    results = {
        "total_samples": total_count,
        "syntax_error_rate": syntax_error_rate,
        "match_rate": match_rate,
        "data_mismatch_rate": data_mismatch_rate,
        "length_mismatch_rate": length_mismatch_rate,
        "syntax_errors": syntax_errors,
        "data_mismatches": data_mismatches,
        "length_mismatches": length_mismatches,
        "error_compare": error_compare,
        "matches": matches,
        "mismatched_queries": mismatched_queries,
    }

    # Write results to target file
    with open(target_file, "w") as f:
        json.dump(results, f, indent=4)


def main():
    parser = argparse.ArgumentParser(description="Compare PPL execution results.")
    parser.add_argument("--label_root", help="Path to the ground truth JSON file")
    parser.add_argument("--results_root", help="Path to the file to evaluate")
    parser.add_argument("--target_root", help="Path to the file to evaluate")
    parser.add_argument("--bench_file", help="Path to write the comparison results")

    args = parser.parse_args()
    os.makedirs(args.target_root, exist_ok=True)

    label_file = os.path.join(args.label_root, args.bench_file)
    results_file = os.path.join(args.results_root, args.bench_file)
    target_file = os.path.join(
        args.target_root,
        (args.label_root + "-" + args.results_root + "-" + args.bench_file).replace(
            "/", "-"
        ),
    )
    print(target_file)
    compare_results(label_file, results_file, target_file)


if __name__ == "__main__":
    main()
