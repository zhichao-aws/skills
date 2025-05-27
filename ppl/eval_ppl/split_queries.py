import json


def split_queries(input_file):
    # Read the input JSON file
    with open(input_file) as f:
        data = json.load(f)

    # Lists to store the split data
    time_related = []
    non_time_related = []

    # Keywords to check for
    time_keywords = ["date", "time"]

    # Process each sample
    for sample in data:
        del sample["table_info"]
        query = sample["query"].lower()

        # Check if query contains any time-related keywords
        is_time_related = any(keyword in query for keyword in time_keywords)

        # Add to appropriate list
        if is_time_related:
            time_related.append(sample)
        else:
            non_time_related.append(sample)

    # Write time-related queries to a file
    with open("dataset/time_related_queries.json", "w") as f:
        json.dump(time_related, f, indent=4)

    # Write non-time-related queries to a file
    with open("dataset/non_time_related_queries.json", "w") as f:
        json.dump(non_time_related, f, indent=4)

    # Print summary
    print(f"Total samples: {len(data)}")
    print(f"Time-related queries: {len(time_related)}")
    print(f"Non-time-related queries: {len(non_time_related)}")


if __name__ == "__main__":
    split_queries("dataset/ppl_eval.json")
