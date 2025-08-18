import os
import re
import json
import random
import string
import urllib.parse
import boto3
from botocore.exceptions import ClientError

## This script links transcripts to their corresponding video files stored in S3 by:
# 1. scans all available video files in the specified S3 bucket.
# 2. matches each transcript to its related video based on filename similarity to vid name.
# 3. generates a unique UUID and stores it with the video’s public URL in DynamoDB.
# 4. injects a video reference tag (!?#Video:<uuid>) into the transcript.
# 5. saves the updated transcript in the output directory.

# AWS setup
region = "us-west-2"
dynamodb = boto3.resource('dynamodb', region_name=region)
dynamo_table = dynamodb.Table('LinksTable')

# Config
TRANSCRIPT_DIR = "final_transcripts_2"
OUTPUT_DIR = "annotated_transcripts"
BUCKET_NAME = 'test-videos'
VIDEO_EXTENSIONS = ['.mp4']

# Utility Functions
def sanitize_filename(name):
    return os.path.splitext(name)[0].replace(" ", "_").replace(".", "_").replace(",", "_")

def generate_short_id(length=5):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def get_unique_uid(max_retries=10):
    for _ in range(max_retries):
        candidate = generate_short_id()
        response = dynamo_table.get_item(Key={'uuid': candidate})
        if 'Item' not in response:
            return candidate
    raise Exception("Failed to generate a unique UID after several attempts.")

def s3_to_http_url(bucket, key):
    encoded_key = urllib.parse.quote(key)
    #TODO: check link correctness
    return f"https://{bucket}.s3.amazonaws.com/{encoded_key}"

def inject_video_tag(transcript_text, uuid):
    lines = transcript_text.splitlines()
    tag_line = f"(!?#Video:{uuid})"
    if lines:
        lines[0] = tag_line  # overwrite the first line
    else:
        lines = [tag_line]  # handle empty file
    return "\n".join(lines)

def get_all_video_keys(bucket_name):
    keys = []
    s3 = boto3.client('s3', region_name=region)
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if any(key.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                keys.append(key)
    return keys

def match_video_to_transcript(video_keys, transcript_name):
    def clean(name):
        return re.sub(r'[^a-zA-Z0-9]', '', name.lower())
    transcript_base = clean(os.path.splitext(transcript_name)[0])
    for key in video_keys:
        video_base = clean(os.path.splitext(os.path.basename(key))[0])
        if video_base in transcript_base or transcript_base in video_base:
            return key
    return None

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    video_keys = get_all_video_keys(BUCKET_NAME)

    for transcript_file in os.listdir(TRANSCRIPT_DIR):
        if not transcript_file.endswith(".txt"):
            continue

        transcript_path = os.path.join(TRANSCRIPT_DIR, transcript_file)
        with open(transcript_path, "r") as f:
            transcript_text = f.read()

        matched_key = match_video_to_transcript(video_keys, transcript_file)
        if not matched_key:
            print(f"No matching video found for {transcript_file}")
            continue

        s3_uri = f"s3://{BUCKET_NAME}/{matched_key}"
        http_link = s3_to_http_url(BUCKET_NAME, matched_key)
        short_uuid = get_unique_uid()

        try:
            dynamo_table.put_item(
                Item={
                    'uuid': short_uuid,
                    'original_link': http_link,
                    'type': 'video-2'
                },
                ConditionExpression='attribute_not_exists(#uuid)',
                ExpressionAttributeNames={'#uuid': 'uuid'}
            )
        except ClientError as e:
            print(f"Error inserting {s3_uri}: {e}")
            continue

        modified = inject_video_tag(transcript_text, short_uuid)
        output_path = os.path.join(OUTPUT_DIR, transcript_file)
        with open(output_path, "w") as f:
            f.write(modified)

        print(f"Updated {transcript_file} with UUID {short_uuid}")

if __name__ == "__main__":
    main()
