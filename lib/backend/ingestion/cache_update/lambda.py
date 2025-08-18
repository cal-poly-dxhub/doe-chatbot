# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time
from typing import Any, Dict, Optional

import boto3
from aws_lambda_powertools import Logger, Tracer

from os.path import splitext, basename, dirname, join

import urllib.parse
import re

logger = Logger()
tracer = Tracer()


dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

CACHE_TABLE_NAME: str = os.environ["CACHE_TABLE_NAME"]
cache_table = dynamodb.Table(CACHE_TABLE_NAME)

ALLOWED_CHARS_PATTERN = re.compile(r'[^a-zA-Z0-9\-_]')

@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event["Records"]
    logger.debug(f"Processing {len(records)} records")

    for record in records:
        event_name = record["eventName"]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        file_dir = dirname(object_key)
        filename = basename(object_key)
        base, ext = splitext(filename)

        tempS3_uri = f"s3://{bucket_name}/{object_key}"
        if event_name.startswith("ObjectRemoved:"):
            logger.info(f"Handling delete event for {tempS3_uri}")
            try:
                # Check if this is an unsanitized file that was replaced by a sanitized version
                sanitized_base = ALLOWED_CHARS_PATTERN.sub('_', base)
                sanitized_filename = f"{sanitized_base}{ext}"
                sanitized_key = join(file_dir, sanitized_filename) if file_dir else sanitized_filename
                sanitized_uri = f"s3://{bucket_name}/{sanitized_key}"
                
                # Only mark as READYDELETE if:
                # 1. The sanitized version doesn't exist in S3, or
                # 2. This is already the sanitized version
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=sanitized_key)
                    # If we get here, the sanitized version exists
                    if sanitized_key == object_key:
                        # This is already the sanitized version, mark as READYDELETE
                        cache_table.update_item(
                            Key={"PK": f"source_location#{tempS3_uri}", "SK": "metadata"},
                            UpdateExpression="SET UpdatedStatus = :ready_delete",
                            ExpressionAttributeValues={":ready_delete": "READYDELETE"},
                            ReturnValues="UPDATED_NEW",
                        )
                    else:
                        # This is the unsanitized version, and sanitized exists, so delete this entry
                        cache_table.delete_item(
                            Key={"PK": f"source_location#{tempS3_uri}", "SK": "metadata"}
                        )
                        logger.info(f"Deleted cache entry for unsanitized file {tempS3_uri}, sanitized version exists")
                except Exception:
                    # Sanitized version doesn't exist, mark this as READYDELETE
                    cache_table.update_item(
                        Key={"PK": f"source_location#{tempS3_uri}", "SK": "metadata"},
                        UpdateExpression="SET UpdatedStatus = :ready_delete",
                        ExpressionAttributeValues={":ready_delete": "READYDELETE"},
                        ReturnValues="UPDATED_NEW",
                    )
            except Exception as e:
                logger.error(f"Failed to handle delete event for {tempS3_uri}: {e}")
                raise e
            continue

        sanitized_base = ALLOWED_CHARS_PATTERN.sub('_', base)
        sanitized_filename = f"{sanitized_base}{ext}"
        sanitized_key = join(file_dir, sanitized_filename) if file_dir else sanitized_filename
        if sanitized_key != object_key:
            logger.info(f"Sanitizing object key: '{object_key}' -> '{sanitized_key}'")

            try:
                # Before copying, check if the sanitized version already exists in the cache table
                sanitized_uri = f"s3://{bucket_name}/{sanitized_key}"
                original_uri = f"s3://{bucket_name}/{object_key}"
                
                # Check if the sanitized version already exists in S3
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=sanitized_key)
                    # If we get here, the sanitized version already exists
                    logger.info(f"Sanitized version {sanitized_uri} already exists, deleting unsanitized {original_uri}")
                    s3_client.delete_object(Bucket=bucket_name, Key=object_key)
                    
                    # Delete the unsanitized entry from cache table if it exists
                    try:
                        cache_table.delete_item(
                            Key={"PK": f"source_location#{original_uri}", "SK": "metadata"}
                        )
                    except Exception as e:
                        logger.warning(f"Failed to delete cache entry for {original_uri}: {e}")
                    
                    # Skip further processing as we're using the existing sanitized version
                    continue
                except Exception:
                    # Sanitized version doesn't exist, proceed with copy and delete
                    pass
                
                # Copy the object to the new sanitized key
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': object_key},
                    Key=sanitized_key
                )
                
                # Delete the original object
                s3_client.delete_object(Bucket=bucket_name, Key=object_key)
                
                # Delete the cache entry for the original object if it exists
                try:
                    cache_table.delete_item(
                        Key={"PK": f"source_location#{original_uri}", "SK": "metadata"}
                    )
                except Exception as e:
                    logger.warning(f"Failed to delete cache entry for {original_uri}: {e}")
                
                object_key = sanitized_key  # Use new key from now on
            except Exception as e:
                logger.error(f"Failed to sanitize and rename object '{object_key}': {e}")
                raise e

        s3_uri = f"s3://{bucket_name}/{object_key}"

        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            content_type = response["ContentType"]
            normalized_type = content_type
            if content_type in {"application/octet-stream", "application/csv", "binary/octet-stream"}:
                inferred_type = infer_content_type_from_extension(object_key)
                if inferred_type:
                    normalized_type = inferred_type

            if normalized_type != content_type:
                logger.info(f"Normalizing content type '{content_type}' -> '{normalized_type}' for {object_key}")
                try:
                    s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': object_key},
                        Key=object_key,
                        ContentType=normalized_type,
                        Metadata=response.get("Metadata", {}),
                        MetadataDirective="REPLACE"
                    )
                    content_type = normalized_type
                except Exception as e:
                    logger.error(f"Failed to update content type for {object_key}: {e}")
                    raise e

            # Reject unsupported types
            supported_types = {
                "text/plain",
                "text/csv",
                "application/pdf",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "video/mp4"
            }
            if content_type not in supported_types:
                logger.debug(f"Skipping {object_key}, unsupported content type: {content_type}")
                continue

            etag = response["ETag"].strip('"')

            item = get_item(s3_uri)
            if item and item.get("ETag") == etag:
                logger.info(f"File {s3_uri} has not changed, skipping update")
                continue

            new_item = {
                "PK": f"source_location#{s3_uri}",
                # The value metadata is placeholder for future extensions to support more use cases
                "SK": "metadata",
                # SK for GSI
                "FileURI": s3_uri,
                "ContentType": content_type,
                "Size": response["ContentLength"],
                "ETag": etag,
                "UpdatedAt": int(time.time()),
                # PK for GSI
                "UpdatedStatus": "UPDATED",
            }

            cache_table.put_item(Item=new_item)
        except Exception as e:
            logger.error(f"Error processing file {object_key} from bucket {bucket_name}: {str(e)}")
            raise e

    return {"statusCode": 200, "body": {"message": f"S3 trigger processed {len(records)} successfully"}}


def get_item(s3_uri: str) -> Optional[Dict[str, Any]]:
    try:
        response = cache_table.get_item(Key={"PK": f"source_location#{s3_uri}", "SK": "metadata"})
        return response.get("Item")  # type: ignore
    except Exception:
        logger.info(f"The file {s3_uri} doesn't exist in DynamoDB")
        return None


def infer_content_type_from_extension(key: str) -> Optional[str]:
    ext = key.lower().split('.')[-1]
    return {
        'txt': 'text/plain',
        'csv': 'text/csv',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'pdf': 'application/pdf',
        'mp4': 'video/mp4',
    }.get(ext)
