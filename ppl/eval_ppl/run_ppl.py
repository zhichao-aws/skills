import os
import json
import argparse
import logging
from tqdm import tqdm
from opensearchpy import OpenSearch
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
)

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


def insert_after_first_pipe(query, insert_text):
    parts = query.split("|", 1)
    if len(parts) < 2:
        return query

    new_query = parts[0] + "|" + insert_text + "|" + parts[1]
    return new_query


def run_ppl(sample):
    query = sample["query"]
    if "start" in sample and sample["start"] is not None:
        query = insert_after_first_pipe(
            query, f"where {sample['date_field']} >= TIMESTAMP('{sample['start']}') "
        )
    if "end" in sample and sample["end"] is not None:
        query = insert_after_first_pipe(
            query, f"where {sample['date_field']} <= TIMESTAMP('{sample['end']}') "
        )
    if "date_field" in sample:
        query = insert_after_first_pipe(query, f"where {sample['date_field']} < NOW() ")
    if "now" in sample:
        query = query.replace("NOW()", sample["now"])
    sample["query"] = query

    try:
        generated_result = client.transport.perform_request(
            "POST", "/_plugins/_ppl", body={"query": query}
        )
        sample["data_rows"] = generated_result["datarows"]
        sample["schema"] = generated_result["schema"]
    except Exception as e:
        logging.error(f"error when execute query: {sample['query']} {e}")
        sample["data_rows"] = "ERROR"
    return sample


def evaluate_ppl(ppl_path):
    with open(ppl_path) as f:
        samples = json.load(f)

    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(
            tqdm(
                executor.map(run_ppl, samples),
                total=len(samples),
                desc="Running PPL",
            )
        )

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate PPL queries.")
    parser.add_argument(
        "--ppl_root",
        type=str,
        help="Root directory for PPL query files",
        default="dataset",
    )
    parser.add_argument(
        "--bench_file",
        type=str,
        help="Input JSON filename with generated PPL queries",
        default="time_related_queries.json",
    )
    parser.add_argument(
        "--output_root",
        type=str,
        help="Output root directory",
        default="results",
    )

    args = parser.parse_args()

    # Construct full input path
    ppl_path = os.path.join(args.ppl_root, args.bench_file)

    # Construct full output path
    output_path = os.path.join(args.output_root, args.bench_file)

    # Ensure output directory exists
    os.makedirs(args.output_root, exist_ok=True)

    results = evaluate_ppl(ppl_path)

    # Save results to a JSON file
    with open(output_path, "w") as f:
        json.dump(results, f, indent=4)

    print(f"Evaluation results saved to {output_path}")
