import os
import re
import boto3
import json
import time
import logging
from typing import Dict, Any, Optional


def setup_bedrock():
    session = boto3.Session(
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    return session.client("bedrock-runtime")


def parse_tagged_string(text):
    """
    解析格式为<tag>value</tag>的字符串为字典
    """
    # 正则表达式匹配<tag>value</tag>模式
    pattern = r"<(\w+)>(.*?)</\1>"

    # 查找所有匹配项
    matches = re.findall(pattern, text, re.DOTALL)

    # 转换为字典
    result = {}
    for tag, value in matches:
        # 处理"null"值
        if value.strip() == "null":
            result[tag] = None
        else:
            result[tag] = value.strip()

    return result


def invoke(q: str, cur_time: str, prompt: str, max_retries: int = 3, initial_delay: float = 1.0) -> Dict[str, Any]:
    """
    Invoke the Bedrock model with retry functionality.
    
    Args:
        q: Question to ask
        cur_time: Current time
        prompt: Prompt template
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay between retries in seconds (default: 1.0)
        
    Returns:
        Dict containing the parsed response
        
    Raises:
        Exception: If all retry attempts fail
    """
    bedrock_client = setup_bedrock()
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps(
                    {
                        "temperature": 0,
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 8192,
                        "messages": [
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": prompt.format(q=q, cur_time=cur_time),
                                    }
                                ],
                            }
                        ],
                    }
                ),
            )
            
            result = json.loads(response.get("body").read())
            generated_text = result["content"][0]["text"]
            return parse_tagged_string(generated_text)
            
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                # Calculate exponential backoff delay
                delay = initial_delay * (2 ** attempt)
                logging.warning(f"Attempt {attempt + 1} failed. Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"All {max_retries} retry attempts failed")
                raise last_exception


def evaluate_time_predictions(time_bench, predictions, print_errors=False):
    """
    Evaluate the accuracy of time series predictions

    Parameters:
    time_bench: List of benchmark data
    predictions: List of prediction results
    print_errors: Whether to print incorrect predictions (default: False)

    Returns:
    dict: Contains accuracy metrics for different data types
    """
    # Initialize counters
    time_series_total = 0
    time_series_correct = 0
    non_time_series_total = 0
    non_time_series_correct = 0
    errors = []

    # Iterate through benchmark and predictions
    for i in range(len(time_bench)):
        truth = time_bench[i]
        pred = predictions[i]

        # Check if it's a time series data
        if truth["start"] is not None and truth["end"] is not None:
            time_series_total += 1
            # Check if the predicted start and end times are correct
            if pred["start"] == truth["start"] and pred["end"] == truth["end"]:
                time_series_correct += 1
            elif print_errors:
                errors.append([i, truth, pred])
        else:
            non_time_series_total += 1
            # For non-time series data, check if predictions are also None
            if pred["start"] is None and pred["end"] is None:
                non_time_series_correct += 1
            elif print_errors:
                errors.append([i, truth, pred])

    # Calculate accuracies
    time_series_accuracy = (
        time_series_correct / time_series_total if time_series_total > 0 else 0
    )
    non_time_series_accuracy = (
        non_time_series_correct / non_time_series_total
        if non_time_series_total > 0
        else 0
    )
    overall_accuracy = (
        (time_series_correct + non_time_series_correct)
        / (time_series_total + non_time_series_total)
        if (time_series_total + non_time_series_total) > 0
        else 0
    )

    # Return results
    return {
        "time_series_accuracy": time_series_accuracy,
        "time_series_correct": time_series_correct,
        "time_series_total": time_series_total,
        "non_time_series_accuracy": non_time_series_accuracy,
        "non_time_series_correct": non_time_series_correct,
        "non_time_series_total": non_time_series_total,
        "overall_accuracy": overall_accuracy,
        "overall_correct": time_series_correct + non_time_series_correct,
        "overall_total": time_series_total + non_time_series_total,
        "errors": errors,
    }
