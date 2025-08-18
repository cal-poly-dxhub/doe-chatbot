# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import tempfile
import json
import time
import uuid
import subprocess
import sys
from typing import List, Optional
import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from . import config

logger = Logger()

def process_video_from_s3(bucket_name: str, object_key: str, media_bucket_name: str) -> List[str]:
    """
    Process a video from S3 through the complete video processing pipeline.
    
    Args:
        bucket_name (str): The S3 bucket containing the video
        object_key (str): The S3 object key for the video file
        media_bucket_name (str): The S3 bucket for storing processed media assets
        
    Returns:
        List[str]: List of processed transcript chunks ready for embedding
    """
    file_uri = f"s3://{bucket_name}/{object_key}"
    logger.info(f"Starting video processing for {file_uri}")
    
    # Validate configuration
    try:
        config.validate_config()
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        raise e
    
    # Create temporary working directory
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Set up working directories
            working_dirs = setup_working_directories(temp_dir)
            
            # Start transcription and wait for completion
            logger.info("Step 1: Starting transcription...")
            transcribe_output_bucket = media_bucket_name
            
            # Generate a unique job name that we'll use consistently
            file_name = os.path.basename(object_key)
            file_base = os.path.splitext(file_name)[0].replace(" ", "_").replace(".", "_")
            unique_job_id = str(uuid.uuid4())[:8]
            job_name = f"{file_base}-{unique_job_id}"
            
            # Use the job name as the output path so the transcript file will be named after the job
            transcribe_output_folder = f"transcripts/{job_name}"
            vid_mapping_file = os.path.join(temp_dir, "vid_name_map.json")
            
            job_name_returned = start_transcription_job(
                bucket_name, object_key, transcribe_output_bucket, 
                transcribe_output_folder, vid_mapping_file, job_name
            )
            
            # Download transcripts
            logger.info("Step 2: Downloading transcripts...")
            local_transcript_dir = working_dirs["transcripts"]
            download_transcripts(transcribe_output_bucket, transcribe_output_folder, local_transcript_dir, job_name)
            
            # Rename transcripts back to original names  
            logger.info("Step 3: Renaming transcripts...")
            rename_transcripts_with_mapping(vid_mapping_file, local_transcript_dir, job_name)
            
            # Process through the video pipeline using existing scripts
            logger.info("Step 4: Running video processing pipeline...")
            script_dir = os.path.dirname(__file__)
            
            # Format transcripts
            formatted_dir = working_dirs["formatted"]
            run_python_script(script_dir, "format_transcript.py", [local_transcript_dir, formatted_dir])
            
            # Generate summaries
            summaries_dir = working_dirs["summaries"]
            run_python_script(script_dir, "generate_llm_summaries.py", [formatted_dir, summaries_dir])
            
            # Insert timestamps
            timestamped_dir = working_dirs["timestamped"]
            run_python_script(script_dir, "insert_timestamps.py", [
                summaries_dir, formatted_dir, "sequential_transcripts_only", timestamped_dir
            ])
            
            # Extract frames and insert image UUIDs
            frames_dir = working_dirs["frames"]
            final_dir = working_dirs["final"]
            temp_videos_dir = working_dirs["temp_videos"]
            run_python_script(script_dir, "frame_extraction.py", [
                timestamped_dir, frames_dir, final_dir, vid_mapping_file, temp_videos_dir, "false"
            ])
            
            # Insert video links
            vid_links_dir = working_dirs["vid_links"]
            run_python_script(script_dir, "insert_vid_links.py", [
                final_dir, vid_links_dir, bucket_name, "false"
            ])
            
            # Compile final transcripts
            logger.info("Step 9: Compiling final transcripts...")
            video_filename = os.path.basename(object_key)
            transcript_chunks = compile_final_transcripts(vid_links_dir, video_filename)
            
            logger.info(f"Video processing completed successfully. Generated {len(transcript_chunks)} chunks.")
            return transcript_chunks
            
        except Exception as e:
            logger.error(f"Video processing failed for {file_uri}: {str(e)}")
            raise e

def setup_working_directories(base_dir: str) -> dict:
    """Set up temporary working directories for video processing."""
    dirs = {
        "transcripts": os.path.join(base_dir, "transcripts"),
        "formatted": os.path.join(base_dir, "formatted"),
        "summaries": os.path.join(base_dir, "summaries"),
        "timestamped": os.path.join(base_dir, "timestamped"),
        "frames": os.path.join(base_dir, "frames"),
        "final": os.path.join(base_dir, "final"),
        "vid_links": os.path.join(base_dir, "vid_links"),
        "temp_videos": os.path.join(base_dir, "temp_videos")
    }
    
    for dir_path in dirs.values():
        os.makedirs(dir_path, exist_ok=True)
    
    return dirs

def start_transcription_job(input_bucket: str, object_key: str, output_bucket: str, 
                          output_folder: str, mapping_file: str, job_name: str) -> str:
    """Start AWS Transcribe job and wait for completion."""
    
    # Create transcribe client
    transcribe_client = config.get_transcribe_client()
    
    # Start transcription
    media_uri = f"s3://{input_bucket}/{object_key}"
    logger.info(f"Starting transcription job: {job_name} for {media_uri}")
    
    try:
        transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat='mp4',
            LanguageCode='en-US',
            OutputBucketName=output_bucket,
            OutputKey=output_folder,
            Settings={
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': 3
            }
        )
    except Exception as e:
        logger.error(f"Failed to start transcription job: {e}")
        raise e
    
    # Save mapping
    job_name_map = {
        object_key: {
            "job_name": job_name,
            "s3_key": object_key,
            "s3_uri": f"s3://{input_bucket}/{object_key}"
        }
    }
    
    with open(mapping_file, "w") as f:
        json.dump(job_name_map, f, indent=2)
    
    # Wait for completion (with timeout)
    max_wait_time = 7200  # 1 hour timeout
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            result = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
            status = result['TranscriptionJob']['TranscriptionJobStatus']
            
            if status == 'COMPLETED':
                logger.info(f"Transcription job {job_name} completed successfully")
                break
            elif status == 'FAILED':
                failure_reason = result['TranscriptionJob'].get('FailureReason', 'Unknown')
                raise Exception(f"Transcription job {job_name} failed: {failure_reason}")
            else:
                logger.info(f"Transcription job {job_name} status: {status}")
                time.sleep(30)
        except Exception as e:
            logger.error(f"Error checking transcription job status: {e}")
            raise e
    else:
        raise Exception(f"Transcription job {job_name} timed out after {max_wait_time} seconds")
    
    return job_name

def download_transcripts(bucket: str, folder: str, local_dir: str, expected_job_name: str):
    """Download transcript files from S3, ensuring we get the correct transcript for our job."""
    s3_client = config.get_s3_client()
    
    try:
        # AWS Transcribe creates the file exactly where we specify it in OutputKey
        # So if folder is "transcripts/job-name", the actual file will be exactly "transcripts/job-name"
        # We need to try both with and without .json extension
        
        # First try without .json extension (most likely)
        expected_transcript_key = folder
        logger.info(f"Looking for specific transcript file: {expected_transcript_key}")
        
        try:
            # Try to download the specific file we expect (without .json)
            local_file = os.path.join(local_dir, f"{expected_job_name}.json")
            s3_client.download_file(bucket, expected_transcript_key, local_file)
            logger.info(f"Successfully downloaded expected transcript: {expected_transcript_key} to {local_file}")
            return
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"Transcript file {expected_transcript_key} not found, trying with .json extension...")
            else:
                raise e
        
        # Try with .json extension as fallback
        expected_transcript_key_with_json = f"{folder}.json"
        logger.info(f"Looking for transcript file with .json extension: {expected_transcript_key_with_json}")
        
        try:
            # Try to download with .json extension
            local_file = os.path.join(local_dir, f"{expected_job_name}.json")
            s3_client.download_file(bucket, expected_transcript_key_with_json, local_file)
            logger.info(f"Successfully downloaded transcript with .json extension: {expected_transcript_key_with_json} to {local_file}")
            return
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Transcript file {expected_transcript_key_with_json} also not found, searching for alternatives...")
            else:
                raise e
        
        # Fallback: search for any files with our folder prefix
        logger.info(f"Searching for transcript files with prefix: {folder}")
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
        
        if 'Contents' not in response:
            logger.warning(f"No transcript files found with prefix s3://{bucket}/{folder}")
            
            # Also try looking in the parent folder for debugging
            parent_prefix = "/".join(folder.split("/")[:-1])
            if parent_prefix:
                logger.info(f"Also checking parent prefix: {parent_prefix}")
                parent_response = s3_client.list_objects_v2(Bucket=bucket, Prefix=parent_prefix)
                for obj in parent_response.get('Contents', []):
                    logger.info(f"Found in parent: {obj['Key']}")
            
            raise Exception(f"No transcript files found for job {expected_job_name} with prefix s3://{bucket}/{folder}")
        
        downloaded_count = 0
        for obj in response['Contents']:
            key = obj['Key']
            logger.info(f"Found object with key: {key}")
            
            # Download any .json files that start with our folder prefix
            if key.startswith(folder) and key.endswith('.json'):
                # Use the job name for the local file to maintain consistency
                local_file = os.path.join(local_dir, f"{expected_job_name}.json")
                logger.info(f"Downloading transcript: {key} -> {local_file}")
                s3_client.download_file(bucket, key, local_file)
                logger.info(f"Downloaded {key} to {local_file}")
                downloaded_count += 1
                break  # We only expect one transcript file per job
        
        if downloaded_count == 0:
            # List all objects to debug what's actually there
            logger.info(f"Debugging: listing all objects with prefix {folder}")
            for obj in response.get('Contents', []):
                logger.info(f"Found object: {obj['Key']}")
            
            raise Exception(f"No JSON transcript files found for job {expected_job_name} with prefix s3://{bucket}/{folder}")
        
    except Exception as e:
        logger.error(f"Error downloading transcripts for job {expected_job_name}: {e}")
        raise e

def rename_transcripts_with_mapping(mapping_file: str, transcript_dir: str, known_job_name: str):
    """Rename transcription files using the mapping."""
    try:
        # Load the mapping
        with open(mapping_file, 'r') as f:
            job_name_map = json.load(f)
        
        # Find the original filename for our known job name
        original_filename = None
        for original_name, job_info in job_name_map.items():
            if job_info['job_name'] == known_job_name:
                original_filename = original_name
                break
        
        if not original_filename:
            raise Exception(f"No mapping found for job name: {known_job_name}")
        
        # Look for the transcript file with our job name
        job_transcript_file = os.path.join(transcript_dir, f"{known_job_name}.json")
        
        if not os.path.exists(job_transcript_file):
            # List all files to debug
            available_files = os.listdir(transcript_dir)
            logger.warning(f"Expected transcript file {job_transcript_file} not found")
            logger.info(f"Available files in transcript directory: {available_files}")
            
            # Try to find any .json file and use it
            json_files = [f for f in available_files if f.endswith('.json')]
            if json_files:
                logger.info(f"Using first available JSON file: {json_files[0]}")
                job_transcript_file = os.path.join(transcript_dir, json_files[0])
            else:
                raise Exception(f"No JSON transcript files found in {transcript_dir}")
        
        # Rename to the original video filename
        original_basename = os.path.splitext(os.path.basename(original_filename))[0]
        # Clean the filename for filesystem safety
        safe_basename = "".join(c for c in original_basename if c.isalnum() or c in (' ', '-', '_')).strip()
        
        new_path = os.path.join(transcript_dir, f"{safe_basename}.json")
        
        if job_transcript_file != new_path:  # Only rename if different
            os.rename(job_transcript_file, new_path)
            logger.info(f"Renamed {os.path.basename(job_transcript_file)} to {os.path.basename(new_path)}")
        else:
            logger.info(f"Transcript file already has correct name: {os.path.basename(new_path)}")
                    
    except Exception as e:
        logger.error(f"Error renaming transcripts: {e}")
        raise e

def run_python_script(script_dir: str, script_name: str, args: List[str]):
    """Run a Python script with the given arguments."""
    script_path = os.path.join(script_dir, script_name)
    cmd = [sys.executable, script_path] + args
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        # Set PYTHONPATH to include the video_chunking directory
        env = os.environ.copy()
        env['PYTHONPATH'] = script_dir + ':' + env.get('PYTHONPATH', '')
        
        result = subprocess.run(
            cmd,
            cwd=script_dir,
            capture_output=True,
            text=True,
            timeout=14400,  # 4 hour timeout (increased for heavy video processing with Nova Pro calls)
            env=env
        )
        
        if result.returncode != 0:
            logger.error(f"Script {script_name} failed with return code {result.returncode}")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            raise Exception(f"Script {script_name} failed: {result.stderr}")
        else:
            logger.info(f"Script {script_name} completed successfully")
            if result.stdout:
                logger.info(f"STDOUT: {result.stdout}")
                
    except subprocess.TimeoutExpired:
        logger.error(f"Script {script_name} timed out")
        raise Exception(f"Script {script_name} timed out after 4 hours")
    except Exception as e:
        logger.error(f"Error running script {script_name}: {e}")
        raise e

def compile_final_transcripts(transcript_dir: str, video_filename: str) -> List[str]:
    """Compile final transcript files into chunks ready for embedding."""
    chunks = []
    
    try:
        # Get base name without extension
        video_base_name = os.path.splitext(video_filename)[0]
        
        # First, look for readable transcript files directly
        readable_transcript_filename = f"{video_base_name}_readable_transcript.txt"
        readable_transcript_path = os.path.join(transcript_dir, readable_transcript_filename)
        
        # Check if the file exists directly
        if os.path.exists(readable_transcript_path):
            logger.info(f"Found direct match: {readable_transcript_filename}")
            with open(readable_transcript_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    chunks.append(content)
                    logger.info(f"Added chunk from direct match: {len(content)} characters")
            return chunks
        
        # If direct match not found, search recursively for any txt files
        logger.info(f"Direct match not found, searching for transcript files matching: {video_base_name}")
        
        # Find all .txt files in the directory
        all_txt_files = []
        for root, dirs, files in os.walk(transcript_dir):
            for filename in files:
                if filename.endswith('.txt'):
                    full_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(full_path, transcript_dir)
                    all_txt_files.append((full_path, relative_path, filename))
        
        # Log all found txt files for debugging
        logger.info(f"All txt files found: {[f[2] for f in all_txt_files]}")
        
        # First try: exact filename match (ignoring path)
        for full_path, relative_path, filename in all_txt_files:
            if video_base_name in filename:
                logger.info(f"Found filename match: {filename}")
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        chunks.append(content)
                        logger.info(f"Added chunk from filename match: {len(content)} characters")
        
        # If still no chunks, try all txt files
        if not chunks and all_txt_files:
            logger.warning(f"No specific transcript chunks found, using all available txt files")
            for full_path, relative_path, filename in all_txt_files:
                try:
                    with open(full_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if content:
                            chunks.append(content)
                            logger.info(f"Added chunk from available file {filename}: {len(content)} characters")
                except Exception as e:
                    logger.warning(f"Error reading file {filename}: {e}")
                    continue
        
        if not chunks:
            logger.warning(f"No transcript chunks found for video {video_filename}")
            
            # As a last resort, check if there are any files in parent directories
            parent_dir = os.path.dirname(transcript_dir)
            logger.info(f"Checking parent directory: {parent_dir}")
            
            for root, dirs, files in os.walk(parent_dir):
                txt_files = [f for f in files if f.endswith('.txt')]
                if txt_files:
                    logger.info(f"Found txt files in parent directory: {txt_files}")
                    # Use the first one as a last resort
                    try:
                        full_path = os.path.join(root, txt_files[0])
                        with open(full_path, 'r', encoding='utf-8') as f:
                            content = f.read().strip()
                            if content:
                                chunks.append(content)
                                logger.info(f"Added chunk from parent directory: {len(content)} characters")
                    except Exception as e:
                        logger.warning(f"Error reading file from parent directory: {e}")
                    break  # Only check one directory level up
        
    except Exception as e:
        logger.error(f"Error compiling transcripts: {e}")
        raise e
    
    return chunks 