import os
import json
from opensearchpy import OpenSearch
from tqdm import tqdm
import argparse
from tqdm import tqdm

# Read the Flow Framework API endpoint from the environment variable
OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "localhost:9200")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD")
ppl_agent_id = "HzU_C5cBgFOPmYzTMS6y"

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


from concurrent.futures import ThreadPoolExecutor


def generate_ppl(samples):
    results = []

    def fetch_ppl(sample):
        ppl = client.transport.perform_request(
            "POST",
            f"/_plugins/_ml/agents/{ppl_agent_id}/_execute",
            body={
                "parameters": {
                    "question": sample["question"],
                    "index": sample["target_index"],
                }
            },
            params={"timeout": 60},
        )["inference_results"][0]["output"][0]["result"]
        return {
            "question": sample["question"],
            "generated_ppl": json.loads(ppl)["ppl"],
            "ground_truth_ppl": sample["query"],
        }

    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(fetch_ppl, samples)))

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate PPL queries.")
    parser.add_argument(
        "--input_file",
        type=str,
        help="Input JSON file with samples",
        default="ppl_eval.json",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        help="Output JSON file for generated PPL queries",
        default="ppl_generated.json",
    )

    args = parser.parse_args()

    with open(args.input_file) as f:
        samples = json.load(f)

    results = generate_ppl(samples)

    with open(args.output_file, "w") as f:
        json.dump(results, f, indent=4)

    print(f"Generated PPL queries saved to {args.output_file}")
