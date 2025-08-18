import boto3
import uuid
import time
import os
import re
import json
import sys 

s3_client = boto3.client('s3', region_name="us-west-2")
transcribe_client = boto3.client('transcribe', region_name="us-west-2")

def list_s3_videos(bucket_name):
    videos = []
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    while response:
        for obj in response.get("Contents", []):
            if obj["Key"].lower().endswith(".mp4"):
                videos.append(obj["Key"])

        if response.get("NextContinuationToken"):
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, ContinuationToken=response["NextContinuationToken"]
            )
        else:
            break
    return videos

def start_transcription(video_file, input_bucket, output_bucket, output_prefix):
    """Starts an AWS Transcribe job for a given S3 video file with a filename-based job name."""

    file_name = os.path.basename(video_file)
    file_base = os.path.splitext(file_name)[0].replace(" ", "_")

    prefix = os.path.dirname(video_file)
    if prefix:
        prefix_clean = re.sub(r'^\d+\.\s*', '', prefix).replace(" ", "_").replace("/", "_")
        job_name = f"{prefix_clean}_{file_base}-{str(uuid.uuid4())}"
    else:
        job_name = f"{file_base}-{str(uuid.uuid4())}"

    media_uri = f"s3://{input_bucket}/{video_file}"
    print(f"Starting transcription job for {video_file} → {job_name}")

    transcribe_client.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': media_uri},
        MediaFormat='mp4',
        LanguageCode='en-US',
        OutputBucketName=output_bucket,
        OutputKey=output_prefix,
        Settings={
            'ShowSpeakerLabels': True,
            'MaxSpeakerLabels': 3
        }
    )
    return job_name

def monitor_transcription(job_name):
    """Poll AWS Transcribe job status until completion or failure."""
    while True:
        result = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
        status = result['TranscriptionJob']['TranscriptionJobStatus']

        if status in ['COMPLETED', 'FAILED']:
            print(f"Transcription job {job_name} status: {status}")
            if status == 'COMPLETED':
                print(f"Transcript available at: {result['TranscriptionJob']['Transcript']['TranscriptFileUri']}")
            break
        else:
            print(f"Job {job_name} still in progress...")
            time.sleep(30)

def main():
    if len(sys.argv) < 5:
        print("Usage: python transcribe.py <input_bucket> <object_key> <output_bucket> <output_prefix> [map_file.json]")
        sys.exit(1)

    input_bucket = sys.argv[1]
    object_key = sys.argv[2] 
    output_bucket = sys.argv[3]
    output_prefix = sys.argv[4]
    mapping_path = sys.argv[5] if len(sys.argv) > 5 else "vid_name_map.json"

    if os.path.exists(mapping_path):
        with open(mapping_path, "r") as f:
            job_name_map = json.load(f)
    else:
        job_name_map = {}

    if object_key in job_name_map:
        print(f"Skipping {object_key}, already recorded.")
        return

    job_name = start_transcription(object_key, input_bucket, output_bucket, output_prefix)
    job_name_map[object_key] = {
        "job_name": job_name,
        "s3_key": object_key,
        "s3_uri": f"s3://{input_bucket}/{object_key}"
    }

    with open(mapping_path, "w") as f:
        json.dump(job_name_map, f, indent=2)

    monitor_transcription(job_name)

if __name__ == "__main__":
    main()
