# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import Optional, List

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from francis_toolkit.utils import find_embedding_model_by_ref_key, get_vector_store
from pydantic import BaseModel
import boto3
from boto3.dynamodb.conditions import Key

logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace=os.getenv("METRICS_NAMESPACE"))


class VectorStoreMgmtRequest(BaseModel):
    purge_data: Optional[bool] = False
    model_ref_key: Optional[str] = None


def get_readydelete_files() -> List[str]:
    """Get list of files marked as readydelete from the cache table."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.getenv("CACHE_TABLE_NAME"))

    try:
        items = []
        last_evaluated_key = None

        while True:
            query_params = {
                "IndexName": "GSI1",
                "KeyConditionExpression": Key("UpdatedStatus").eq("READYDELETE"),
                "ProjectionExpression": "FileURI",
            }

            if last_evaluated_key:
                query_params["ExclusiveStartKey"] = last_evaluated_key

            response = table.query(**query_params)
            items.extend(response.get("Items", []))

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        return [item["FileURI"] for item in items]
    except Exception as e:
        logger.error(f"Error getting readydelete files: {e}")
        return []


def delete_from_cache_table(file_uri: str) -> None:
    """Delete an item from the cache table."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.getenv("CACHE_TABLE_NAME"))

    try:
        table.delete_item(
            Key={
                "PK": f"source_location#{file_uri}",
                "SK": "metadata"
            }
        )
        logger.info(f"Successfully deleted {file_uri} from cache table")
    except Exception as e:
        logger.error(f"Error deleting {file_uri} from cache table: {e}")
        raise e


def delete_vectors_for_file(vector_store, file_uri: str) -> None:
    """Delete vectors associated with a file from the vector store."""
    with vector_store._session_maker() as session:
        # Get all document IDs for the file
        results = session.query(vector_store.EmbeddingStore.id).filter(
            vector_store.EmbeddingStore.document_source_uri == file_uri
        ).all()
        
        if results:
            doc_ids = [r[0] for r in results]
            logger.info(f"Deleting {len(doc_ids)} vectors for file {file_uri}")
            vector_store.delete(ids=doc_ids)
        else:
            logger.info(f"No vectors found for file {file_uri}")
        
        # Always delete from cache table, regardless of whether vectors were found or not
        try:
            delete_from_cache_table(file_uri)
        except Exception as e:
            logger.error(f"Error deleting {file_uri} from cache table: {e}")
            raise e


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def handler(event: dict, context: LambdaContext) -> None:
    request = VectorStoreMgmtRequest(**event)

    embedding_model = find_embedding_model_by_ref_key(request.model_ref_key)
    if embedding_model is None:
        raise ValueError(f"Embedding model {request.model_ref_key} not found")

    vector_store = get_vector_store(embedding_model)

    if request.purge_data:
        logger.info("Purging vector store")
        vector_store.delete_collection()
        vector_store.create_collection()
    else:
        # Handle READYDELETE files by deleting them from vector store and cache table
        # These files have been deleted from S3 and should be cleaned up from the system
        readydelete_files = get_readydelete_files()
        failed_files = []
        
        for file_uri in readydelete_files:
            try:
                delete_vectors_for_file(vector_store, file_uri)
            except Exception as e:
                logger.error(f"Error deleting vectors for file {file_uri}: {e}")
                failed_files.append(file_uri)
        
        if failed_files:
            logger.warning(f"Failed to process {len(failed_files)} files: {failed_files}")
