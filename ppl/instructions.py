import boto3
import hashlib
import json
import os
import random
import time
import argparse
from botocore.exceptions import ClientError

from tqdm import tqdm


def setup_bedrock():
    session = boto3.Session(
        # aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        # aws_secret_access_key=os.environ.get("AWS_SECRET_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    return session.client("bedrock-runtime")


def generate_sample(bedrock_client, data, args):
    # Randomly select examples based on provided range
    min_examples, max_examples = args.examples_range
    selected_examples = random.sample(
        data["examples"], k=random.randint(min_examples, max_examples)
    )

    # Build the prompt
    prompt = f"""
    index name: {data["index_name"]}
    index mapping:{data["index_mapping"]}\n\n
    """
    for ex in selected_examples:
        prompt += f"<request>{ex['request']}</request><ppl>{ex['ppl']}</ppl><python>{ex['python']}</python><full_desc>{ex['full_description']}</full_desc>\n\n"
    prompt += """
    Now follow the above examples, generate 5 samples of simple search request, ppl query, python code and full description for the simple search request. 
    1. The query, ppl and python code should match the data type of index fields.
    2. Make sure you do data type cast in python code. Even redundant.
    """

    os.makedirs(args.output_dir, exist_ok=True)
    file_path = os.path.join(
        args.output_dir, f"{hashlib.sha256(prompt.encode()).hexdigest()}.json"
    )
    if os.path.exists(file_path):
        return

    retries = 0
    while retries <= args.max_retries:
        try:
            response = bedrock_client.invoke_model(
                modelId=args.model_id,
                body=json.dumps(
                    {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": args.max_tokens,
                        "messages": [
                            {
                                "role": "user",
                                "content": [{"type": "text", "text": prompt}],
                            }
                        ],
                    }
                ),
            )

            # Process response
            result = json.loads(response.get("body").read())
            generated_text = result["content"][0]["text"]
            with open(file_path, "w") as f:
                f.write(generated_text)
            return

        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                if retries < args.max_retries:
                    # Exponential backoff with jitter
                    wait_time = (2**retries) + random.random()
                    print(
                        f"Throttling detected, retrying in {wait_time:.2f} seconds (attempt {retries+1}/{args.max_retries})"
                    )
                    time.sleep(wait_time)
                    retries += 1
                else:
                    print(
                        f"Maximum retries reached ({args.max_retries}). Skipping this sample."
                    )
                    return None
            else:
                print(f"Error generating sample: {e}")
                return None
        except Exception as e:
            print(f"Unexpected error generating sample: {e}")
            return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate samples using Bedrock models"
    )

    # Model configuration
    parser.add_argument(
        "--model-id",
        default="anthropic.claude-3-5-sonnet-20240620-v1:0",
        help="Model ID to use for generation",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=100000,
        help="Maximum number of tokens to generate",
    )

    # Input/output paths
    parser.add_argument(
        "--sample-data-path",
        default="sample-logs.json",
        help="Path to the sample logs JSON file",
    )
    parser.add_argument(
        "--output-dir",
        default="output/raw/sample-logs",
        help="Directory to save generated outputs",
    )

    # Generation parameters
    parser.add_argument(
        "--num-samples", type=int, default=50, help="Number of samples to generate"
    )
    parser.add_argument(
        "--examples-range",
        type=int,
        nargs=2,
        default=[3, 4],
        help="Range of examples to select (min max)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum number of retry attempts when throttled",
    )

    return parser.parse_args()


# Usage example
if __name__ == "__main__":
    args = parse_args()
    bedrock_client = setup_bedrock()

    with open(args.sample_data_path) as f:
        data = json.load(f)

    for i in tqdm(range(args.num_samples)):
        generate_sample(bedrock_client, data, args)
