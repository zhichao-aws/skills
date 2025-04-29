import os
import json
import re
import time

from opensearchpy import OpenSearch
from datetime import datetime


SCROLL_TIME = "5m"
BATCH_SIZE = 1000


def export_to_jsonl(index_name, file_path):
    os_client = OpenSearch(os.environ.get("HOSTS"))

    try:
        # Get the initial scroll ID
        result = os_client.search(
            index=index_name,
            scroll=SCROLL_TIME,
            size=BATCH_SIZE,
            body={"query": {"match_all": {}}},
        )
        scroll_id = result["_scroll_id"]
        hits = result["hits"]["hits"]

        # Counter for progress tracking
        total_docs = result["hits"]["total"]["value"]
        processed_docs = 0
        start_time = time.time()

        print(f"Total documents to export: {total_docs}")

        # Open file and write documents
        with open(file_path, "w", encoding="utf-8") as f:
            while hits:
                # Process current batch
                for hit in hits:
                    # Write document to file
                    f.write(json.dumps(hit["_source"]) + "\n")
                    processed_docs += 1

                # Print progress
                if processed_docs % 10000 == 0:
                    elapsed_time = time.time() - start_time
                    docs_per_second = processed_docs / elapsed_time
                    print(
                        f"Processed {processed_docs}/{total_docs} documents "
                        f"({(processed_docs/total_docs*100):.2f}%) "
                        f"- {docs_per_second:.2f} docs/sec"
                    )

                # Get next batch of results
                result = os_client.scroll(scroll_id=scroll_id, scroll=SCROLL_TIME)
                scroll_id = result["_scroll_id"]
                hits = result["hits"]["hits"]

        # Final progress update
        elapsed_time = time.time() - start_time
        docs_per_second = processed_docs / elapsed_time
        print(f"\nExport completed!")
        print(f"Total documents exported: {processed_docs}")
        print(f"Total time: {elapsed_time:.2f} seconds")
        print(f"Average speed: {docs_per_second:.2f} docs/sec")
        print(f"Output file: {file_path}")

    except Exception as e:
        print(f"Error during export: {e}")
        raise
    finally:
        # Clear scroll
        try:
            os_client.clear_scroll(scroll_id=scroll_id)
        except:
            pass


def parse_examples(input_text):
    """
    Parse examples from text containing <request>, <ppl>, <python>, and <full_desc> tags.

    Args:
        input_text (str): Text containing tagged examples

    Returns:
        list: List of dictionaries with keys 'request', 'ppl', 'python', 'full_desc'
    """
    # Split the input text into individual examples (separated by blank lines)
    examples = [example.strip() for example in input_text.strip().split("\n\n")]
    result = []

    for example in examples:
        if not example:  # Skip empty examples
            continue

        # Extract content between tags using regex
        request_match = re.search(r"<request>(.*?)</request>", example, re.DOTALL)
        ppl_match = re.search(r"<ppl>(.*?)</ppl>", example, re.DOTALL)
        python_match = re.search(r"<python>(.*?)</python>", example, re.DOTALL)
        full_desc_match = re.search(r"<full_desc>(.*?)</full_desc>", example, re.DOTALL)

        # Check if all patterns were found
        if all([request_match, ppl_match, python_match, full_desc_match]):
            example_dict = {
                "request": request_match.group(1),
                "ppl": ppl_match.group(1),
                "python": python_match.group(1),
                "full_desc": full_desc_match.group(1),
            }
            result.append(example_dict)

    return result
