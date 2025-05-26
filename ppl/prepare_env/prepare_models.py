import os
import json
import time
from opensearchpy import OpenSearch

# Read the Flow Framework API endpoint from the environment variable
OPENSEARCH_HOST = os.environ.get("OPENSEARCH_HOST", "localhost:9200")
OPENSEARCH_USER = os.environ.get("OPENSEARCH_USER")
OPENSEARCH_PASSWORD = os.environ.get("OPENSEARCH_PASSWORD")
WORKFLOW_NAME = "Claude and T2PPL Models with Test Agent"
AGENT_NAME = "ppl agent"

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

# Check if the client is connected
if client.ping():
    print("Connected to OpenSearch cluster")
else:
    print("Could not connect to OpenSearch cluster")
    exit()

# ML commons settings
ml_commons_settings = {
    "persistent": {
        "plugins.ml_commons.only_run_on_ml_node": False,
        "plugins.ml_commons.trusted_connector_endpoints_regex": [
            "^https://runtime\\.sagemaker\\..*[a-z0-9-]\\.amazonaws\\.com/.*$",
            "^https://bedrock.*",
        ],
    }
}

# flow framework request body
workflow_template = {
    "name": WORKFLOW_NAME,
    "description": "Create models using Claude on BedRock and T2PPL on SageMaker, and register PPL tool to test agent",
    "use_case": "REGISTER_REMOTE_MODEL",
    "version": {"template": "1.0.0", "compatibility": ["2.18.0", "3.0.0"]},
    "workflows": {
        "provision": {
            "user_params": {},
            "nodes": [
                {
                    "id": "create_claude_connector",
                    "type": "create_connector",
                    "previous_node_inputs": {},
                    "user_inputs": {
                        "credential": {
                            "access_key": os.environ.get("AWS_ACCESS_KEY_ID"),
                            "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                        },
                        "parameters": {
                            "endpoint": "bedrock-runtime.us-east-1.amazonaws.com",
                            "content_type": "application/json",
                            "auth": "Sig_V4",
                            "max_tokens_to_sample": "8000",
                            "service_name": "bedrock",
                            "temperature": "0.0000",
                            "response_filter": "$.content[0].text",
                            "region": "us-east-1",
                            "anthropic_version": "bedrock-2023-05-31",
                        },
                        "version": "1",
                        "name": "Claude haiku runtime Connector",
                        "protocol": "aws_sigv4",
                        "description": "The connector to BedRock service for claude model",
                        "actions": [
                            {
                                "action_type": "predict",
                                "method": "POST",
                                "url": "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-3-haiku-20240307-v1:0/invoke",
                                "headers": {
                                    "content-type": "application/json",
                                    "x-amz-content-sha256": "required",
                                },
                                "request_body": '{"messages":[{"role":"user","content":[{"type":"text","text":"${parameters.prompt}"}]}],"anthropic_version":"${parameters.anthropic_version}","max_tokens":${parameters.max_tokens_to_sample}}',
                            }
                        ],
                    },
                },
                {
                    "id": "register_claude_model",
                    "type": "register_remote_model",
                    "previous_node_inputs": {"create_claude_connector": "connector_id"},
                    "user_inputs": {
                        "name": "claude-haiku",
                        "description": "Claude model",
                        "deploy": True,
                    },
                },
                {
                    "id": "create_t2ppl_connector",
                    "type": "create_connector",
                    "previous_node_inputs": {},
                    "user_inputs": {
                        "credential": {
                            "access_key": os.environ.get("PPL_ACCESS_KEY", ""),
                            "secret_key": os.environ.get("PPL_SECRET_KEY", ""),
                        },
                        "parameters": {
                            "region": "us-east-1",
                            "service_name": "sagemaker",
                            "input_docs_processed_step_size": 10,
                        },
                        "version": "1",
                        "name": "sagemaker: t2ppl",
                        "protocol": "aws_sigv4",
                        "description": "Test connector for Sagemaker t2ppl model",
                        "actions": [
                            {
                                "action_type": "predict",
                                "method": "POST",
                                "headers": {"content-type": "application/json"},
                                "url": "https://runtime.sagemaker.us-east-1.amazonaws.com/endpoints/production-olly/invocations",
                                "request_body": '{"prompt":"${parameters.prompt}"}',
                            }
                        ],
                    },
                },
                {
                    "id": "register_t2ppl_model",
                    "type": "register_remote_model",
                    "previous_node_inputs": {"create_t2ppl_connector": "connector_id"},
                    "user_inputs": {
                        "name": "t2ppl",
                        "description": "T2PPL model",
                        "deploy": True,
                    },
                },
            ],
        }
    },
}


def create_workflow(client, workflow_template):
    response = client.transport.perform_request(
        "POST", "/_plugins/_flow_framework/workflow", body=workflow_template
    )
    return response


def provision_workflow(client, workflow_id):
    response = client.transport.perform_request(
        "POST", f"/_plugins/_flow_framework/workflow/{workflow_id}/_provision"
    )
    return response


def get_workflow_status(client, workflow_id):
    response = client.transport.perform_request(
        "GET", f"/_plugins/_flow_framework/workflow/{workflow_id}/_status"
    )
    return response


if __name__ == "__main__":
    try:
        # Check if the workflow exists
        try:
            existing_workflows = client.transport.perform_request(
                "GET",
                "/_plugins/_flow_framework/workflow/_search",
                body={"query": {"match": {"name": "claude model"}}},
            )
            workflow_id = existing_workflows["hits"]["hits"][0]["_id"]
            print(f"workflow existed: {workflow_id}")
        except:
            workflow_id = None

        if workflow_id is None:
            # Create the workflow if it doesn't exist
            create_response = create_workflow(client, workflow_template)
            workflow_id = create_response["workflow_id"]
            print(f"Workflow created with ID: {workflow_id}")

        # Get the current status of the workflow
        status_response = get_workflow_status(client, workflow_id)
        current_status = status_response["state"]

        if current_status == "NOT_STARTED":
            # Provision the workflow if it's not already complete
            provision_response = provision_workflow(client, workflow_id)
            print(
                f"Workflow provisioning started. Status: {provision_response['status']}"
            )

            # Check status every 3 seconds until complete
            while True:
                time.sleep(3)
                status_response = get_workflow_status(client, workflow_id)
                current_status = status_response["state"]
                print(f"Current status: {current_status}")
                if current_status == "COMPLETE":
                    break

        assert current_status == "COMPLETED"
        # Print the final status
        final_status = get_workflow_status(client, workflow_id)

        # Extract and print the required resource IDs
        t2ppl_model_id = None
        claude_model_id = None
        for resource in final_status["resources_created"]:
            if (
                resource["workflow_step_id"] == "register_t2ppl_model"
                and resource["workflow_step_name"] == "deploy_model"
            ):
                t2ppl_model_id = resource["resource_id"]
            elif (
                resource["workflow_step_id"] == "register_claude_model"
                and resource["workflow_step_name"] == "deploy_model"
            ):
                claude_model_id = resource["resource_id"]

        print(f"PPL model id: {t2ppl_model_id}")
        print(f"claude model id: {claude_model_id}")

        try:
            existing_agent = client.transport.perform_request(
                "GET",
                "/_plugins/_ml/agents/_search",
                body={"query": {"match": {"name": AGENT_NAME}}},
            )
            agent_id = existing_agent["hits"]["hits"][0]["_id"]
            print(f"agent existed: {agent_id}")
        except:
            agent_id = None

        if agent_id is None:
            agent_id = client.transport.perform_request(
                "POST",
                "/_plugins/_ml/agents/_register",
                body={
                    "name": "ppl agent",
                    "type": "flow",
                    "description": "this is a test agent",
                    "memory": {"type": "conversation_index"},
                    "tools": [
                        {
                            "type": "PPLTool",
                            "name": "TransferQuestionToPPLAndExecuteTool",
                            "description": "Use this tool to transfer natural language to generate PPL and execute PPL to query inside. Use this tool after you know the index name, otherwise, call IndexRoutingTool first. The input parameters are: {index:IndexName, question:UserQuestion}",
                            "parameters": {
                                "model_id": t2ppl_model_id,
                                "model_type": "FINETUNE",
                                "execute": False,
                            },
                        }
                    ],
                },
            )
            print(f"register ppl agent. agent id: {agent_id}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
