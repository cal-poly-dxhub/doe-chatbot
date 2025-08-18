import os
import re
import json
import random
import string
import urllib.parse
import boto3
from botocore.exceptions import ClientError
import ffmpeg

## This script processes video transcripts with embedded timestamps through the following steps:
# 1. extracts vid frames at timestamps in transcript using ffmpeg.
# 2. creates S3 with structured naming for frames generated.
# 3. generates a unique UUID for each frame and saves it in DynamoDB.
# 4. inserts image references with UUIDs back into the transcript.
# 5. saves modified transcripts and a full JSON mapping of timestamps to image URLs and UUIDs.

# AWS setup
region = "us-west-2"
dynamodb = boto3.resource('dynamodb', region_name=region)
dynamo_table = dynamodb.Table('LinksTable')

# Config
TRANSCRIPT_DIR = "annotated_transcripts"
VIDEO_DIR = "flattened_videos"
OUTPUT_DIR = "final_frames"
BUCKET_NAME = 'video-frames-1'

# Utility Functions
def sanitize_filename(name):
    return os.path.splitext(name)[0].replace(" ", "_").replace(".", "_").replace(",", "_")

def extract_timestamps(transcript_text):
    return [float(match) for match in re.findall(r'\(!\?#Timestamp:(\d+\.?\d*)\)', transcript_text)]

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
    return f"https://{bucket}.s3.amazonaws.com/{encoded_key}"

def inject_image_tags(text, timestamp_to_uuid):
    def replacer(match):
        timestamp = float(match.group(1))
        uuid = timestamp_to_uuid.get(timestamp)
        if uuid:
            return f"(!?#Timestamp:{timestamp}) (!?#Image:{uuid})"
        else:
            return match.group(0)
    return re.sub(r'\(!\?#Timestamp:(\d+\.?\d*)\)', replacer, text)

# Core Frame Extraction + DynamoDB + Mapping + Transcript Injection
def extract_frames(video_path, timestamps, frame_output_dir, s3_base_url, s3_bucket, video_filename):
    os.makedirs(frame_output_dir, exist_ok=True)
    unique_timestamps = sorted(set(timestamps))
    frame_cache = {}
    frame_data = []

    for t in unique_timestamps:
        actual_time = max(0, t)
        sanitized_time = str(actual_time).replace('.', '_')
        frame_name = f"frame_{sanitized_time}.jpg"
        frame_path = os.path.join(frame_output_dir, frame_name)
        s3_key = f"{sanitize_filename(video_filename)}/{frame_name}"
        s3_uri = f"s3://{s3_bucket}/{s3_key}"
        s3_url = s3_to_http_url(s3_bucket, s3_key)

        if t not in frame_cache:
            if not os.path.exists(frame_path):
                (
                    ffmpeg
                    .input(video_path, ss=actual_time)
                    .output(frame_path, vframes=1)
                    .overwrite_output()
                    .run(quiet=True)
                )

            short_uuid = get_unique_uid()
            try:
                dynamo_table.put_item(
                    Item={
                        'uuid': short_uuid,
                        'original_link': s3_url,
                        'type': 'video-image'
                    },
                    ConditionExpression='attribute_not_exists(#uuid)',
                    ExpressionAttributeNames={'#uuid': 'uuid'}
                )
            except ClientError as e:
                print(f"Error inserting {s3_uri}: {e}")
                continue

            frame_cache[t] = {
                'frame_url': s3_url,
                'uuid': short_uuid
            }

        frame_data.append({
            'timestamp': t,
            'frame_url': frame_cache[t]['frame_url'],
            'uuid': frame_cache[t]['uuid']
        })

    return frame_data

# Main Pipeline
def main():
    mapping = {}

    for transcript_file in os.listdir(TRANSCRIPT_DIR):
        if not transcript_file.endswith(".txt"):
            continue

        base_name = os.path.splitext(transcript_file)[0]
        sanitized_transcript = sanitize_filename(transcript_file)

        video_candidates = [
            f for f in os.listdir(VIDEO_DIR)
            if f.endswith(".mp4") and sanitize_filename(f) in sanitized_transcript
        ]

        if not video_candidates:
            print(f"No matching video found for: {transcript_file}")
            continue
        if len(video_candidates) > 1:
            print(f"Multiple matching videos found for {transcript_file}: {video_candidates}")
            continue

        video_file = video_candidates[0]
        video_path = os.path.join(VIDEO_DIR, video_file)
        transcript_path = os.path.join(TRANSCRIPT_DIR, transcript_file)

        with open(transcript_path, "r") as f:
            transcript_text = f.read()

        timestamps = extract_timestamps(transcript_text)
        frame_output_dir = os.path.join(OUTPUT_DIR, sanitize_filename(base_name))
        s3_base_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{sanitize_filename(base_name)}"

        print(f"Processing {video_file} with {len(timestamps)} timestamps...")
        frame_data = extract_frames(
            video_path,
            timestamps,
            frame_output_dir,
            s3_base_url,
            s3_bucket=BUCKET_NAME,
            video_filename=video_file
        )

        mapping[video_file] = frame_data

        # Inject [(!?#Image:<uuid>)] into transcript text
        timestamp_to_uuid = {entry['timestamp']: entry['uuid'] for entry in frame_data}
        modified_transcript = inject_image_tags(transcript_text, timestamp_to_uuid)

        # Save the imagelink transcripts     
        os.makedirs("final_transcripts", exist_ok=True)
        output_transcript_path = os.path.join("final_transcripts/", transcript_file)
        with open(output_transcript_path, "w") as out_f:
            out_f.write(modified_transcript)


        print(f"Updated transcript saved to {output_transcript_path}")

    # Save final mapping
    with open("mapping.json", "w") as f:
        json.dump(mapping, f, indent=2)
    print("Done. Mapping saved to mapping.json")

if __name__ == "__main__":
    main()
