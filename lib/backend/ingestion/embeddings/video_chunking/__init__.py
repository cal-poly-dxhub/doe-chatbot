# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Video processing pipeline for the Chatbot ingestion system.

This package provides functionality to process video files through a complete pipeline:
1. Transcription using Amazon Transcribe
2. Transcript formatting and structuring
3. LLM-based summarization
4. Timestamp insertion
5. Frame extraction and image processing
6. Video link insertion
7. Final compilation for embedding

The main entry point is the process_video_from_s3 function in video_processor.py
"""

from .video_processor import process_video_from_s3

__all__ = ['process_video_from_s3'] 