# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import csv
import io
import json
import os
import time
from typing import List, Optional

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError
from francis_toolkit.utils import find_embedding_model_by_ref_key, get_vector_store
from langchain.docstore.document import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pydantic import BaseModel

from langchain_aws.embeddings.bedrock import BedrockEmbeddings
from langchain_experimental.text_splitter import SemanticChunker

from docx_chunking.docxChunker import extract_text_with_images_from_docx
from io import BytesIO

import re

from pdf_chunking.pdfChunker import process_pdf_from_s3, extract_raw_text_from_pdf_s3
from video_chunking.video_processor import process_video_from_s3

logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace=os.getenv("METRICS_NAMESPACE"))


dynamodb = boto3.resource("dynamodb")


CHUNK_SIZE_DOC_SPLIT = int(os.getenv("CHUNK_SIZE_DOC_SPLIT", 100000))
OVERLAP_FOR_DOC_SPLIT = int(os.getenv("OVERLAP_FOR_DOC_SPLIT", 200))
CONCAT_CSV_ROWS = os.getenv("CONCAT_CSV_ROWS", "false").lower() == "true"
LINKS_TABLE_NAME: str = os.environ["LINKS_TABLE_NAME"]
linksTable = dynamodb.Table(LINKS_TABLE_NAME)

class FileEmbeddingsRequest(BaseModel):
    FileURI: str
    ContentType: str
    model_ref_key: Optional[str] = None


def read_from_s3(bucket_name: str, object_key: str) -> bytes:
    """
    Read a file from an S3 bucket and return its content as bytes.

    Args:
    bucket_name (str): The name of the S3 bucket.
    object_key (str): The key (path) of the object in the bucket.

    Returns:
    content: The content of the file.

    Raises:
    Exception: If there's an error reading the file.
    """
    s3_client = boto3.client("s3")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response["Body"].read()
        return content
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")  # noqa: B904


def load_metadata(bucket_name: str, object_key: str) -> dict:
    """Load metadata from a metadata.json file."""
    metadata = {}
    metadata_key = f"{object_key}.metadata.json"
    try:
        content = read_from_s3(bucket_name, metadata_key)
        metadata.update(json.loads(content.decode()))
        return metadata
    except Exception:
        return metadata


def process_text_embeddings(content: str) -> List[Document]:
    return [Document(page_content=content)]


def process_csv_embeddings(content: str) -> List[Document]:
    csv_stream = io.StringIO(content)

    # Create a CSV reader object from the BytesIO object
    csv_reader = csv.DictReader(csv_stream)
    text_list = []

    for row in csv_reader:
        text_list.append("\n".join([f"{key}: {value}" for key, value in row.items()]))

    if CONCAT_CSV_ROWS:
        documents = [Document(page_content="\n".join(text_list))]
    else:
        documents = [Document(page_content=text) for text in text_list]

    return documents


def update_ingested_time(file_uri: str) -> bool:
    """
    Update the IngestedAt attribute to the current time for an item in DynamoDB.

    :return: True if update was successful, False otherwise
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.getenv("CACHE_TABLE_NAME"))

    current_time = int(time.time())  # Current time in Unix timestamp format

    update_expression = """
    SET IngestedAt = :ingested_time,
        UpdatedStatus = :updated_status
    """
    expression_attribute_values = {":ingested_time": current_time, ":updated_status": "INGESTED"}

    try:
        table.update_item(
            Key={"PK": f"source_location#{file_uri}", "SK": "metadata"},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )
        return True

    except ClientError as e:
        logger.error(f"Error updating item: {e.response['Error']['Message']}")
        return False

def chunking_preserve_links(text: str, chunkSize: int, overlap: int) -> List[str]:
    special_block_pattern = re.compile(r'(!\?#(?:Image|Video|Timestamp):[^\n]*\n?)')
    chunkslist = []
    pos = 0
    textlen = len(text)
    while pos < textlen:
        end = min(pos + chunkSize, textlen)
        extendedEnd = end
        # Extend the chunk if a special block (e.g., URL) crosses the boundary.
        for match in special_block_pattern.finditer(text, pos, min(end + 500, textlen)):
            if match.start() < end < match.end():
                extendedEnd = match.end()
        
        # Append the current chunk.
        chunkslist.append(text[pos:extendedEnd])
        
        # Calculate the starting position for the next chunk.
        newpos = extendedEnd - overlap
        if newpos <= pos:
            newpos = pos + chunkSize
        
        # Adjust newpos so it doesn't fall in the middle of a special block.
        # If newpos is within any special block, advance newpos to the end of that block.
        for match in special_block_pattern.finditer(text):
            if match.start() < newpos < match.end():
                newpos = match.end()
                break

        pos = newpos

    return chunkslist


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler(capture_response=False)
@metrics.log_metrics(capture_cold_start_metric=True)
def handler(event: dict, context: LambdaContext) -> dict:
    request = FileEmbeddingsRequest(**event)
    file_uri = request.FileURI
    content_type = request.ContentType

    # Check if the file is marked as READYDELETE in the cache table
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.getenv("CACHE_TABLE_NAME"))
    try:
        response = table.get_item(
            Key={"PK": f"source_location#{file_uri}", "SK": "metadata"}
        )
        item = response.get("Item")
        if item and item.get("UpdatedStatus") == "READYDELETE":
            logger.info(f"File {file_uri} is marked as READYDELETE, skipping embeddings generation")
            return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "SKIPPED_READYDELETE"}
    except Exception as e:
        logger.warning(f"Error checking file status in DynamoDB: {e}")
        # Continue processing as normal if we can't check the status

    embedding_model = find_embedding_model_by_ref_key(request.model_ref_key)
    if embedding_model is None:
        raise ValueError(f"Embedding model {request.model_ref_key} not found")

    bucket_name, object_key = file_uri.replace("s3://", "").split("/", 1)
    try:
        raw_content = read_from_s3(bucket_name, object_key)
    except Exception as e:
        logger.error(f"Failed to read file {file_uri} from S3: {e}")
        # Don't mark as ingested - this could be a temporary S3 issue that should be retried
        return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "ERROR_READING_FILE"}

    documents = []
    metadata = load_metadata(bucket_name, object_key)

    # add additional metadata
    metadata["source"] = file_uri
    metadata["create_timestamp"] = int(time.time() * 1000)
    metadata["embeddings_model_id"] = embedding_model.modelId

    if content_type == "text/plain":
        documents = process_text_embeddings(raw_content.decode("utf-8"))

    elif content_type in ["text/csv", "application/csv"]:
        documents = process_csv_embeddings(raw_content.decode("utf-8"))
    
    elif content_type == "application/pdf":
        # Use the PDF chunking strategy with fallback
        cleaned_text_chunks = process_pdf_from_s3(bucket_name, object_key)
        if not cleaned_text_chunks:
            logger.warning(f"PDF processing with filters resulted in no valid chunks for {file_uri}. Trying raw text extraction as fallback.")
            try:
                # Fallback: extract raw text as a single chunk
                raw_text = extract_raw_text_from_pdf_s3(bucket_name, object_key)
                if raw_text.strip() and len(raw_text.split()) >= 10:  # Minimum 10 words
                    logger.info(f"Raw text extraction successful for {file_uri}. Using single chunk with {len(raw_text.split())} words.")
                    cleaned_text_chunks = [raw_text.strip()]
                else:
                    logger.warning(f"Raw text extraction failed or produced insufficient content for {file_uri}.")
                    update_ingested_time(file_uri)
                    return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "NO_VALID_CHUNKS"}
            except Exception as e:
                logger.error(f"Raw text extraction failed for {file_uri}: {e}")
                # Don't mark as ingested - this is a technical failure that should be retried
                return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "RAW_TEXT_EXTRACTION_FAILED"}
        documents = [Document(page_content=chunk, metadata=metadata) for chunk in cleaned_text_chunks]

    elif content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        docx_stream = BytesIO(raw_content)
        extracted_text = extract_text_with_images_from_docx(docx_stream, linksTable)
        documents = [Document(page_content=extracted_text)]

    elif content_type == "video/mp4":
        # Use the video processing pipeline
        try:
            media_bucket_name = os.getenv("MEDIA_BUCKET_NAME")
            if not media_bucket_name:
                logger.error("MEDIA_BUCKET_NAME environment variable not set for video processing")
                # Don't mark as ingested - this is a configuration error that should be retried
                return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "MISSING_MEDIA_BUCKET"}
            
            video_chunks = process_video_from_s3(bucket_name, object_key, media_bucket_name)
            if not video_chunks:
                logger.warning(f"Video processing resulted in no chunks for {file_uri}")
                # Don't mark as ingested - this might be a temporary issue that should be retried
                return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "NO_VIDEO_CHUNKS"}
            
            documents = [Document(page_content=chunk, metadata=metadata) for chunk in video_chunks]
            logger.info(f"Video processing successful for {file_uri}. Generated {len(documents)} documents.")
            
        except Exception as e:
            logger.error(f"Video processing failed for {file_uri}: {str(e)}")
            # Don't mark as ingested - processing failed and should be retried
            return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "VIDEO_PROCESSING_FAILED"}

    else:
        # This shouldn't occur since unsupported types are filtered out in the ingestion pipeline.
        # Treat this as a fallback case.
        logger.debug(f"Unsupported content type: {content_type} for {file_uri}")
        # Mark the file as ingested to avoid reprocessing
        update_ingested_time(file_uri)
        return {"FileURI": file_uri, "EmbeddingsGenerated": 0}

    
    text_length = sum(len(doc.page_content) for doc in documents)

    if text_length > 25000:
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=10000,
            chunk_overlap=1000,
            length_function=len,
        )
        splitchunks = text_splitter.create_documents(
            [doc.page_content for doc in documents],
            metadatas=[metadata for _ in documents]
        )

        chunks = []
        for doc in splitchunks:
            res = chunking_preserve_links(doc.page_content, 2000, 300)
            chunks.extend([Document(page_content=chunk,metadata=metadata) for chunk in res])

    else:
        chunks = [Document(page_content=doc.page_content, metadata=metadata) for doc in documents]

    # Check if we have any chunks to process
    if not chunks:
        logger.warning(f"No chunks to process for {file_uri}. This may indicate a file with little to no text content.")
        update_ingested_time(file_uri)
        return {"FileURI": file_uri, "EmbeddingsGenerated": 0, "Status": "NO_CHUNKS_TO_PROCESS"}

    # don't create tables in the ingesiton pipeline as it may lead to race condition due to Map iterations
    vector_store = get_vector_store(embedding_model)
    embeddings = vector_store.add_documents(documents=chunks, document_source_uri=file_uri)

    update_ingested_time(file_uri)

    return {"FileURI": file_uri, "EmbeddingsGenerated": len(embeddings)}
