import os
import json
import argparse
import logging
from tqdm import tqdm
from opensearchpy import OpenSearch

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
    if "date_field" in sample:
        query = insert_after_first_pipe(
            sample["query"], f"where {sample['date_field']} < NOW() "
        )
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


from concurrent.futures import ThreadPoolExecutor


def evaluate_ppl(ppl_eval_file):
    with open(ppl_eval_file) as f:
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
        "--ppl_file",
        type=str,
        help="Input JSON file with generated PPL queries",
        default="dataset/time_related_queries.json",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        help="output file",
        default=None,
    )

    args = parser.parse_args()
    if args.output_file is None:
        args.output_file = os.path.join("results", args.ppl_file.replace("/", "-"))

    results = evaluate_ppl(args.ppl_file)

    # Save results to a JSON file
    with open(args.output_file, "w") as f:
        json.dump(results, f, indent=4)
