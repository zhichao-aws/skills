import os
import json
from opensearchpy import OpenSearch
from tqdm import tqdm
import argparse
from concurrent.futures import ThreadPoolExecutor

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


def generate_ppl(samples):
    results = []

    def fetch_ppl(sample: dict):
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
        sample["origin_query"] = sample.pop("query")
        sample["query"] = json.loads(ppl)["ppl"]
        return sample

    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(fetch_ppl, samples)))

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate PPL queries.")
    parser.add_argument(
        "--input_root",
        type=str,
        help="Input root directory",
        default="dataset",
    )
    parser.add_argument(
        "--bench_file",
        type=str,
        help="Input JSON filename",
        default="time_related_queries.json",
    )
    parser.add_argument(
        "--output_root",
        type=str,
        help="Output root directory",
        default="generated_ppls",
    )

    args = parser.parse_args()

    # Construct full paths
    input_path = os.path.join(args.input_root, args.bench_file)
    output_path = os.path.join(args.output_root, args.bench_file)

    # Ensure output directory exists
    os.makedirs(args.output_root, exist_ok=True)

    with open(input_path) as f:
        samples = json.load(f)

    results = generate_ppl(samples)

    with open(output_path, "w") as f:
        json.dump(results, f, indent=4)

    print(f"Generated PPL queries saved to {output_path}")
