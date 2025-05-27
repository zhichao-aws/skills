import os
import json
import logging
from datetime import datetime, timedelta
from opensearchpy import OpenSearch, helpers

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# OpenSearch connection details
OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "localhost:9200")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD")

# Configuration for time adjustment
TIME_DELTA_DAYS = 30  # Adjust this value as needed

anchor_time_fields = {
    "ecommerce": ["order_date"],
    "flight": ["timestamp"],
    "log_index": ["timestamp"],
    "otel_logs": ["startTime"],
    "sso_log": ["@timestamp"],
}

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


def read_dataset_file(file_path):
    with open(file_path, "r") as f:
        return [json.loads(line) for line in f]


def read_mapping_file(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def create_or_recreate_index(index_name, mapping):
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
    client.indices.create(index_name, body=mapping)


def get_date_fields(mapping, parent_key=""):
    date_fields = []

    # Check for properties in the mapping
    if "properties" in mapping:
        for field, properties in mapping["properties"].items():
            full_key = f"{parent_key}.{field}" if parent_key else field

            # Check if this field is a date type
            if properties.get("type") in ["date", "date_nanos"]:
                date_fields.append(full_key)

            # Recursively check nested properties
            if "properties" in properties:
                date_fields.extend(get_date_fields(properties, full_key))

            # Check for fields within the field definition
            if "fields" in properties:
                for sub_field, sub_properties in properties["fields"].items():
                    if sub_properties.get("type") in ["date", "date_nanos"]:
                        date_fields.append(f"{full_key}.{sub_field}")

                    # Recursively check properties within fields
                    if "properties" in sub_properties:
                        date_fields.extend(
                            get_date_fields(sub_properties, f"{full_key}.{sub_field}")
                        )

    return date_fields


def adjust_time_fields(documents, date_fields, anchor_field, time_delta):
    """
    Adjust time fields in documents based on an anchor field.

    Args:
        documents: List of document dictionaries
        date_fields: List of field paths (using dot notation for nested fields)
        anchor_field: The field to use as time anchor
        time_delta: Number of days to shift time backwards from current time

    Returns:
        The modified documents list
    """
    if not documents:
        return documents

    try:
        # Helper function to get a field value using dot notation path
        def get_field_value(doc, field_path):
            parts = field_path.split(".")
            current = doc
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
            return current

        # Helper function to set a field value using dot notation path
        def set_field_value(doc, field_path, value):
            parts = field_path.split(".")
            current = doc
            for i, part in enumerate(parts[:-1]):
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return False

            if isinstance(current, dict) and parts[-1] in current:
                current[parts[-1]] = value
                return True

            return False

        # Find documents that have the anchor field
        valid_anchors = []
        for doc in documents:
            anchor_value = get_field_value(doc, anchor_field)
            if anchor_value and isinstance(anchor_value, str):
                try:
                    valid_anchors.append(
                        datetime.fromisoformat(anchor_value.rstrip("Z"))
                    )
                except (ValueError, AttributeError):
                    continue

        if not valid_anchors:
            logging.warning(f"No valid anchor times found for field: {anchor_field}")
            return documents

        min_anchor_time = min(valid_anchors)
        max_anchor_time = max(valid_anchors)

        logging.info(f"Time range: {min_anchor_time} to {max_anchor_time}")

        # Calculate the time adjustment
        now = datetime.utcnow()
        target_time = now - timedelta(days=time_delta)
        time_difference = target_time - min_anchor_time

        print(f"Time difference for adjustment: {time_difference}")

        # Adjust all date fields
        adjusted_count = 0
        for doc in documents:
            for field in date_fields:
                # Get the field value
                field_value = get_field_value(doc, field)
                if field_value and isinstance(field_value, str):
                    try:
                        original_time = datetime.fromisoformat(field_value[:23])
                        adjusted_time = original_time + time_difference
                        if set_field_value(doc, field, adjusted_time.isoformat() + "Z"):
                            adjusted_count += 1
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Error adjusting time for field {field}: {e}")

        logging.info(
            f"Adjusted {adjusted_count} fields across {len(documents)} documents. Min anchor time: {min_anchor_time}, Target time: {target_time}"
        )
    except Exception as e:
        logging.error(f"Error in time adjustment: {e}")
        import traceback

        traceback.print_exc()

    return documents


def bulk_index_documents(index_name, documents):
    actions = [{"_index": index_name, "_source": doc} for doc in documents]
    helpers.bulk(client, actions)
    client.indices.refresh(index_name)


def process_and_index_files(dry_run=False):
    dataset_dir = "dataset"
    mapping_dir = "mapping"

    for filename in os.listdir(dataset_dir):
        if filename.endswith(".jsonl"):
            index_name = os.path.splitext(filename)[0]
            dataset_path = os.path.join(dataset_dir, filename)
            mapping_path = os.path.join(mapping_dir, f"{index_name}.json")

            logging.info(f"Processing {index_name}...")

            try:
                # Read dataset and mapping
                documents = read_dataset_file(dataset_path)
                mapping = read_mapping_file(mapping_path)

                # Get date fields from mapping
                date_fields = get_date_fields(mapping["mappings"])
                logging.info(f"Date fields for {index_name}: {date_fields}")

                # Get anchor field for the current index
                anchor_field = anchor_time_fields.get(index_name, [])[0]

                # if anchor_field and anchor_field in date_fields:
                #     # Adjust time fields
                #     documents = adjust_time_fields(
                #         documents, date_fields, anchor_field, TIME_DELTA_DAYS
                #     )
                # else:
                #     logging.warning(
                #         f"No valid anchor field found for {index_name}. Skipping time adjustment."
                #     )

                if not dry_run:
                    # Create or recreate the index with the correct mapping
                    create_or_recreate_index(index_name, mapping)

                    # Bulk index the adjusted documents
                    bulk_index_documents(index_name, documents)

                    logging.info(f"Indexed {len(documents)} documents in {index_name}")
                else:
                    logging.info(
                        f"Dry run: Would index {len(documents)} documents in {index_name}"
                    )

            except Exception as e:
                logging.error(f"Error processing {index_name}: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Process and index files with time adjustment"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Perform a dry run without indexing"
    )
    args = parser.parse_args()

    if client.ping():
        process_and_index_files(dry_run=args.dry_run)
    else:
        logging.error("Could not connect to OpenSearch cluster")
