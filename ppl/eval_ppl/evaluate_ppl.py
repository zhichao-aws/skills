import os
import json
import argparse
from tqdm import tqdm
from opensearchpy import OpenSearch

# Read the Flow Framework API endpoint from the environment variable
OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "localhost:9200")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD")

# Initialize the OpenSearch client
client = OpenSearch(
    hosts=OPENSEARCH_HOST,
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD)
    if OPENSEARCH_USER and OPENSEARCH_PASSWORD
    else None,
    use_ssl=True if OPENSEARCH_USER and OPENSEARCH_PASSWORD else False,
    verify_certs=False,
    ssl_show_warn=False,
)


def run_ppl(query):
    return json.dumps(
        client.transport.perform_request(
            "POST", "/_plugins/_ppl", body={"query": query}
        )["datarows"]
    )


from concurrent.futures import ThreadPoolExecutor


def evaluate_ppl(ppl_eval_file):
    with open(ppl_eval_file) as f:
        samples = json.load(f)

    results = []
    total_correct = 0
    total_samples = len(samples)

    def run_and_compare(sample):
        generated_result = run_ppl(sample["generated_ppl"])
        ground_truth_result = run_ppl(sample["ground_truth_ppl"])
        is_correct = generated_result == ground_truth_result
        return {
            "question": sample["question"],
            "generated_result": generated_result,
            "ground_truth_result": ground_truth_result,
            "is_correct": is_correct,
        }, is_correct

    with ThreadPoolExecutor() as executor:
        results_and_correctness = list(
            tqdm(
                executor.map(run_and_compare, samples),
                total=len(samples),
                desc="Evaluating PPL",
            )
        )

    for result, is_correct in results_and_correctness:
        results.append(result)
        if is_correct:
            total_correct += 1

    accuracy = total_correct / total_samples
    return results, accuracy


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate PPL queries.")
    parser.add_argument(
        "--ppl_eval_file",
        type=str,
        help="Input JSON file with generated PPL queries",
        default="ppl_generated.json",
    )
    parser.add_argument(
        "--ppl_eval_results",
        type=str,
        help="output file",
        default="ppl_eval_results.json",
    )

    args = parser.parse_args()

    results, accuracy = evaluate_ppl(args.ppl_eval_file)

    print(f"Accuracy: {accuracy:.2%}")

    # Save results to a JSON file
    with open(args.ppl_eval_results, "w") as f:
        json.dump({"results": results, "accuracy": accuracy}, f, indent=2)

    print(f"Results saved to {ppl_eval_results}")
