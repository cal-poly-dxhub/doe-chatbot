#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import sys
import traceback
import boto3
import uuid
from typing import Dict, Any

# Import the existing lambda handler logic
import embeddings_lambda

def write_result_to_s3(result: Dict[str, Any], event: Dict[str, Any]) -> None:
    """
    Write the task result to S3 in the same format and location as the original DistributedMap resultWriter.
    This ensures seamless integration with the existing pipeline.
    """
    try:
        s3_client = boto3.client('s3')
        
        # Get the processed bucket name from environment variables
        bucket_name = os.environ.get('PROCESSED_BUCKET_NAME')
        if not bucket_name:
            print("WARNING: PROCESSED_BUCKET_NAME not set, cannot write result to S3", file=sys.stderr)
            return
        
        # Get execution name from Step Functions context (passed through environment variables)
        execution_name = os.environ.get('EXECUTION_NAME')
        if not execution_name:
            # Fallback to extracting from event if environment variable is not set
            if 'Execution' in event and 'Name' in event['Execution']:
                execution_name = event['Execution']['Name']
            elif '_executionName' in event:
                execution_name = event['_executionName']
            else:
                execution_name = f"ecs-task-{uuid.uuid4().hex[:8]}"
        
        # Generate a unique file name for this task result (similar to what DistributedMap does)
        file_uri = result.get('FileURI', 'unknown')
        safe_file_uri = file_uri.replace('s3://', '').replace('/', '_')
        result_file_name = f"{safe_file_uri}_{uuid.uuid4().hex[:8]}.json"
        
        # Construct the S3 key in the same format as the original DistributedMap resultWriter
        s3_key = f"ingestion_output/{execution_name}/sf-results/{result_file_name}"
        
        # Write the result to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(result),
            ContentType='application/json'
        )
        
        print(f"Successfully wrote result to s3://{bucket_name}/{s3_key}")
        
    except Exception as e:
        print(f"WARNING: Failed to write result to S3: {str(e)}", file=sys.stderr)
        print(f"WARNING: Result will not be persisted: {json.dumps(result)}", file=sys.stderr)

def main():
    """
    Main entrypoint for ECS container.
    Reads input from environment variable and processes embeddings.
    """
    try:
        # Get input JSON from environment variable set by Step Functions
        input_json = os.environ.get('INPUT_JSON')
        if not input_json:
            print("ERROR: INPUT_JSON environment variable not set", file=sys.stderr)
            sys.exit(1)
        
        # Parse the input JSON
        try:
            event = json.loads(input_json)
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse INPUT_JSON: {e}", file=sys.stderr)
            sys.exit(1)
        
        # Create a mock Lambda context for compatibility
        class MockLambdaContext:
            def __init__(self):
                self.function_name = "embeddings-ecs-task"
                self.function_version = "1.0"
                self.invoked_function_arn = f"arn:aws:ecs:{os.environ.get('AWS_REGION', 'us-east-1')}:123456789012:task/embeddings"
                self.memory_limit_in_mb = int(os.environ.get('MEMORY_LIMIT', '32768'))
                self.remaining_time_in_millis = lambda: 999999999  # Unlimited time for ECS
                self.log_group_name = "/aws/ecs/embeddings"
                self.log_stream_name = "embeddings-stream"
                self.aws_request_id = "ecs-task-id"
        
        context = MockLambdaContext()
        
        print(f"Processing embeddings for input: {json.dumps(event, indent=2)}")
        
        # Call the existing Lambda handler
        result = embeddings_lambda.handler(event, context)
        
        # Check if the processing actually succeeded by examining the Status field
        status = result.get('Status')
        embeddings_generated = result.get('EmbeddingsGenerated', 0)
        
        # Define error statuses that should be treated as failures
        error_statuses = [
            'VIDEO_PROCESSING_FAILED',
            'NO_VALID_CHUNKS',
            'RAW_TEXT_EXTRACTION_FAILED',
            'NO_CHUNKS_TO_PROCESS',
            'ERROR_READING_FILE',
            'MISSING_MEDIA_BUCKET',
            'NO_VIDEO_CHUNKS'
        ]
        
        if status in error_statuses:
            print(f"Embeddings processing failed with status '{status}': {json.dumps(result, indent=2)}")
            # Write result to S3 even for failures so we can track them
            write_result_to_s3(result, event)
            # Exit with error code to indicate failure
            sys.exit(1)
        elif status == 'SKIPPED_READYDELETE':
            print(f"Embeddings processing skipped (file marked for deletion): {json.dumps(result, indent=2)}")
            write_result_to_s3(result, event)
            sys.exit(0)
        else:
            print(f"Embeddings processing completed successfully: {json.dumps(result, indent=2)}")
            # Write result to S3 in the same format as the original DistributedMap resultWriter
            write_result_to_s3(result, event)
            # Exit with success code
            sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: Embeddings processing failed: {str(e)}", file=sys.stderr)
        print(f"ERROR: Traceback: {traceback.format_exc()}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 