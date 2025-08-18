# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Configuration module for video processing pipeline.
Handles environment variables and AWS service configurations.
"""

import os
import boto3
from botocore.config import Config

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'us-west-2')
REGION_NAME = os.getenv('REGION_NAME', AWS_REGION)

# S3 Configuration
MEDIA_BUCKET_NAME = os.getenv('MEDIA_BUCKET_NAME')
VIDEO_FRAMES_BUCKET = os.getenv('VIDEO_FRAMES_BUCKET', MEDIA_BUCKET_NAME)

# DynamoDB Configuration
LINKS_TABLE_NAME = os.getenv('LINKS_TABLE_NAME', 'LinksTable')

# Video Processing Configuration
VIDEO_FRAME_MEDIA_TYPE = os.getenv('VIDEO_FRAME_MEDIA_TYPE', 'video-image')
DRY_RUN = os.getenv('VIDEO_PROCESSING_DRY_RUN', 'false').lower() == 'true'

# AWS Clients
def get_s3_client():
    """Get configured S3 client."""
    return boto3.client('s3', region_name=REGION_NAME)

def get_dynamodb_resource():
    """Get configured DynamoDB resource."""
    return boto3.resource('dynamodb', region_name=REGION_NAME)

def get_transcribe_client():
    """Get configured Transcribe client."""
    return boto3.client('transcribe', region_name=REGION_NAME)

def get_bedrock_runtime_client():
    """Get configured Bedrock Runtime client."""
    config = Config(read_timeout=1000)

    return boto3.client('bedrock-runtime', region_name=REGION_NAME, config=config)

def get_links_table():
    """Get the links table for storing media UUIDs."""
    dynamodb = get_dynamodb_resource()
    return dynamodb.Table(LINKS_TABLE_NAME)

# Validation
def validate_config():
    """Validate that required configuration is present."""
    errors = []
    
    if not MEDIA_BUCKET_NAME:
        errors.append("MEDIA_BUCKET_NAME environment variable is required")
    
    if not LINKS_TABLE_NAME:
        errors.append("LINKS_TABLE_NAME environment variable is required")
    
    if errors:
        raise ValueError(f"Configuration validation failed: {', '.join(errors)}")
    
    return True 