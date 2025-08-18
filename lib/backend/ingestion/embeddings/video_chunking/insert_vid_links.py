import os
import re
import json
import random
import string
import urllib.parse
import boto3
from botocore.exceptions import ClientError
import sys

# Import config for AWS client configuration
try:
    from . import config
except ImportError:
    # For standalone execution
    import config

# === UTILITY FUNCTIONS ===

def sanitize_filename(name):
    return os.path.splitext(name)[0].replace(" ", "_").replace(".", "_").replace(",", "_")

def generate_short_id(length=5):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def get_unique_uid(max_retries=10):
    dynamo_table = config.get_links_table()
    for _ in range(max_retries):
        candidate = generate_short_id()
        response = dynamo_table.get_item(Key={'uuid': candidate})
        if 'Item' not in response:
            return candidate
    raise Exception("Failed to generate a unique UID after several attempts.")

def s3_to_http_url(bucket, key):
    encoded_key = urllib.parse.quote(key)
    return f"https://{bucket}.s3.amazonaws.com/{encoded_key}"

def inject_video_tag(transcript_text, uuid):
    lines = transcript_text.splitlines()
    tag_line = f"(!?#Video:{uuid})"
    if lines:
        lines[0] = tag_line
    else:
        lines = [tag_line]
    return "\n".join(lines)

def get_all_video_keys(bucket_name):
    keys = []
    s3 = config.get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith('.mp4'):
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

# === MAIN WORKFLOW ===

def main():
    if len(sys.argv) != 5:
        print("Usage: python insert_vid_links.py <TRANSCRIPT_DIR> <OUTPUT_DIR> <BUCKET_NAME> <DRY_RUN:true|false>")
        sys.exit(1)

    TRANSCRIPT_DIR = sys.argv[1]
    OUTPUT_DIR = sys.argv[2]
    BUCKET_NAME = sys.argv[3]
    DRY_RUN = sys.argv[4].lower() == 'true'

    dynamo_table = config.get_links_table()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[INFO] Scanning S3 bucket '{BUCKET_NAME}' for video files...")
    video_keys = get_all_video_keys(BUCKET_NAME)
    print(f"[INFO] Found {len(video_keys)} video files.\n")

    # Will store UUIDs per matched video key to avoid duplicates
    mapping = {}

    for root, _, files in os.walk(TRANSCRIPT_DIR):
        for file in files:
            if not file.endswith(".txt"):
                continue

            transcript_path = os.path.join(root, file)
            relative_subdir = os.path.relpath(root, TRANSCRIPT_DIR)
            output_subdir = os.path.join(OUTPUT_DIR, relative_subdir)
            os.makedirs(output_subdir, exist_ok=True)
            output_path = os.path.join(output_subdir, file)

            print(f"\n--- Processing: {file} ---")
            with open(transcript_path, "r") as f:
                transcript_text = f.read()

            matched_key = match_video_to_transcript(video_keys, file)
            if not matched_key:
                print(f"No video match found for: {file}")
                continue

            if matched_key in mapping:
                short_uuid = mapping[matched_key]['uuid']
                http_link = mapping[matched_key]['s3_url']
                print(f"[REUSE] Reusing UUID {short_uuid} for video: {matched_key}")
            else:
                http_link = s3_to_http_url(BUCKET_NAME, matched_key)
                short_uuid = get_unique_uid()

                if DRY_RUN:
                    print(f"[DRY RUN] Would insert UUID {short_uuid} -> {http_link}")
                else:
                    try:
                        dynamo_table.put_item(
                            Item={
                                'uuid': short_uuid,
                                'original_link': http_link,
                                'type': 'video-reference'
                            },
                            ConditionExpression='attribute_not_exists(#uuid)',
                            ExpressionAttributeNames={'#uuid': 'uuid'}
                        )
                        print(f"[INSERTED] UUID {short_uuid} → {http_link}")
                    except ClientError as e:
                        print(f"[ERROR] DynamoDB insert failed: {e}")
                        continue

                mapping[matched_key] = {
                    'uuid': short_uuid,
                    's3_url': http_link
                }

            modified_text = inject_video_tag(transcript_text, short_uuid)
            with open(output_path, "w") as out_f:
                out_f.write(modified_text)
            print(f"[SAVED] Transcript with video UUID saved to {output_path}")

    # Save the mapping between transcript files and UUIDs
    with open("video_uuid_mapping.json", "w") as f:
        json.dump(mapping, f, indent=2)
    print("\nDone! Mapping saved to video_uuid_mapping.json.")

if __name__ == "__main__":
    main()
