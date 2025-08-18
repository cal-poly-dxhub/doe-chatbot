# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import re
import json
import random
import string
import urllib.parse
import boto3
import sys
from botocore.exceptions import ClientError
import ffmpeg
import base64
import time

# Import config for AWS client configuration
try:
    from . import config
except ImportError:
    # For standalone execution
    import config

# AWS clients (configured through config module)
s3_client = config.get_s3_client()
dynamo_table = config.get_links_table()

# === UTILITY FUNCTIONS ===

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
        return f"(!?#Timestamp:{timestamp}) (!?#Image:{uuid})" if uuid else match.group(0)
    return re.sub(r'\(!\?#Timestamp:(\d+\.?\d*)\)', replacer, text)

def download_s3_video(s3_uri, local_path):
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1]
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3_client.download_file(bucket, key, local_path)

def busy_wait_delay(duration_seconds=90):
    """
    Perform busy waiting for the specified duration to prevent ECS timeout
    while dealing with AI model throttling. Default is 90 seconds (1.5 minutes).
    """
    print(f"Starting busy wait delay for {duration_seconds} seconds to handle throttling...")
    start_time = time.time()
    counter = 0
    
    while time.time() - start_time < duration_seconds:
        # Perform CPU-intensive operations to keep ECS busy
        counter += 1
        # Simple mathematical operations to consume CPU cycles
        result = sum(i * i for i in range(1000))
        
        # Print progress every 15 seconds
        elapsed = time.time() - start_time
        if counter % 50000 == 0:  # Adjust frequency as needed
            remaining = duration_seconds - elapsed
            # print(f"Busy waiting... {elapsed:.1f}s elapsed, {remaining:.1f}s remaining")
        
        # Small sleep to prevent 100% CPU usage while still staying active
        time.sleep(0.001)
    
    print(f"Busy wait delay completed after {time.time() - start_time:.1f} seconds")

def call_nova_pro(image_path):
# this function invokes nova pro to determine relevance of image
    MODEL_ID = "us.amazon.nova-pro-v1:0"

    try:
        # Read and base64-encode local image
        with open(image_path, "rb") as f:
            image_bytes = f.read()
            image_base64 = base64.b64encode(image_bytes).decode("utf-8")

        # Build system + user prompts
        system_list = [{
            "text": "You are a detail-oriented analyst. Given an image, describe its contents in two detailed sentences. Then classify it as {{RELEVANT}} or {{IRRELEVANT}} based on its connection to the [CLIENT] system."
        }]

        message_list = [{
            "role": "user",
            "content": [
                {
                    "image": {
                        "format": "jpeg", 
                        "source": {
                            "bytes": image_base64
                        }
                    }
                },
                {
                    "text": """Describe the contents of the image in one sentence. Be extremely detailed and pay attention to minor details. Mostly focus on the content within the image - 
                    Details like the colors of the webpage aren't important, so don't include it. A good description will call out the locations of all the details,
                    and will be concise yet detailed. Aim to write 2 complete sentences of the description of the image. The description should be in this format: {{DESCRIPTION: Your description here...}}
                    After the summary is complete, determine whether or not the image is in any way relevant to or connected to the [CLIENT] system. 
                    If it is, append {{RELEVANT}} at the end of your response. If it isn't, write {{IRRELEVANT}}."""
                }
            ]
        }]

        # Inference parameters
        request_config = {
            "schemaVersion": "messages-v1",
            "messages": message_list,
            "system": system_list,
            "inferenceConfig": {
                "maxTokens": 500,
                "topP": 0.9,
                "temperature": 0.3
            }
        }

        # Add busy waiting delay before Nova Pro call to handle throttling
        busy_wait_delay(5)  # 15 seconds busy wait - reduced from 90s

        # Invoke Bedrock
        client = config.get_bedrock_runtime_client()
        response = client.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(request_config),
            accept="application/json",
            contentType="application/json"
        )

        result = json.loads(response["body"].read())
        output_text = result["output"]["message"]["content"][0]["text"]
        print(output_text)
        return "RELEVANT" if "{{RELEVANT}}" in output_text else "IRRELEVANT"
    except Exception as e:
        print(f"Error processing image {image_path}: {str(e)}")
        # Return IRRELEVANT for any failed image to skip it
        return "IRRELEVANT"


def extract_frames(video_path, timestamps, frame_output_dir, s3_bucket, video_filename):
    # extract frames with respect to timestamp + relevancy, generates uuid for s3 link
    os.makedirs(frame_output_dir, exist_ok=True)
    unique_timestamps = sorted(set(timestamps))
    frame_cache = {}
    frame_data = []

    print(f"Starting frame extraction for {len(unique_timestamps)} unique timestamps")
    
    for i, t in enumerate(unique_timestamps):
        print(f"Processing frame {i+1}/{len(unique_timestamps)} at timestamp {t:.2f}s")
        try:
            actual_time = max(0, t)
            sanitized_time = str(actual_time).replace('.', '_')
            frame_name = f"frame_{sanitized_time}.jpg"
            frame_path = os.path.join(frame_output_dir, frame_name)
            s3_key = f"{sanitize_filename(video_filename)}/{frame_name}"
            s3_url = s3_to_http_url(s3_bucket, s3_key)

            if t not in frame_cache:
                try:
                    if not os.path.exists(frame_path):
                        (
                            ffmpeg
                            .input(video_path, ss=actual_time)
                            .output(frame_path, vframes=1)
                            .overwrite_output()
                            .run(quiet=True)
                        )
                except Exception as e:
                    print(f"Error extracting frame at {t:.2f}s: {str(e)}")
                    continue  # Skip this frame and move to the next one
                
                relevance = call_nova_pro(frame_path)
                if relevance != "RELEVANT":
                    print(f"Skipping frame at {t:.2f}s - marked IRRELEVANT")
                    continue  # skip uuid generation + insertion into transcript for this frame
                
                short_uuid = get_unique_uid()
                dry_run = config.DRY_RUN
                if dry_run:
                    print(f"[DRY RUN] Would upload {frame_path} to s3://{s3_bucket}/{s3_key}")
                    print(f"[DRY RUN] Would insert UUID {short_uuid} → {s3_url}")
                    frame_cache[t] = {
                        'frame_url': s3_url,
                        'uuid': short_uuid
                    }
                else:
                    try:
                        # Upload the frame to S3
                        print(f"Uploading {frame_path} to s3://{s3_bucket}/{s3_key}")
                        s3_client.upload_file(frame_path, s3_bucket, s3_key)
                        print(f"Successfully uploaded frame to S3: {s3_key}")
                        
                        # Store the UUID mapping in DynamoDB
                        dynamo_table.put_item(
                            Item={
                                'uuid': short_uuid,
                                'original_link': s3_url,
                                'type': config.VIDEO_FRAME_MEDIA_TYPE
                            },
                            ConditionExpression='attribute_not_exists(#uuid)',
                            ExpressionAttributeNames={'#uuid': 'uuid'}
                        )
                        frame_cache[t] = {
                            'frame_url': s3_url,
                            'uuid': short_uuid
                        }
                        print(f"Successfully created UUID mapping: {short_uuid} → {s3_url}")
                    except ClientError as e:
                        print(f"Error uploading frame or inserting UUID mapping: {e}")
                        continue
                    except Exception as e:
                        print(f"Unexpected error processing frame: {e}")
                        continue
            
            # Only append to frame_data if we have a valid entry in frame_cache
            if t in frame_cache:
                frame_data.append({
                    'timestamp': t,
                    'frame_url': frame_cache[t]['frame_url'],
                    'uuid': frame_cache[t]['uuid']
                })
        except Exception as e:
            print(f"Error processing timestamp {t}: {str(e)}")
            # Continue with the next timestamp
            continue

    return frame_data

# === MAIN WORKFLOW ===
def main():
    if len(sys.argv) != 7:
        print("Usage: python frame_extraction.py <TRANSCRIPT_DIR> <OUTPUT_DIR> <FINAL_TRANSCRIPT_DIR> <VIDEO_LOOKUP_JSON> <TEMP_VIDEO_DIR> <DRY_RUN:true|false>")
        sys.exit(1)

    TRANSCRIPT_DIR = sys.argv[1]
    OUTPUT_DIR = sys.argv[2]
    FINAL_TRANSCRIPT_DIR = sys.argv[3]
    VIDEO_LOOKUP_JSON = sys.argv[4]
    TEMP_VIDEO_DIR = sys.argv[5]
    dry_run_arg = sys.argv[6].lower()
    
    # Set the DRY_RUN flag in config
    config.DRY_RUN = dry_run_arg == "true"

    BUCKET_NAME = config.VIDEO_FRAMES_BUCKET

    with open(VIDEO_LOOKUP_JSON, "r") as f:
        video_lookup = json.load(f)
    print(f"Loaded video lookup map with {len(video_lookup)} entries.")
    mapping = {}
    os.makedirs(TEMP_VIDEO_DIR, exist_ok=True)
    os.makedirs(FINAL_TRANSCRIPT_DIR, exist_ok=True)

    for root, _, files in os.walk(TRANSCRIPT_DIR):
        for transcript_file in files:
            if not transcript_file.endswith(".txt"):
                continue

            transcript_path = os.path.join(root, transcript_file)
            base_name = os.path.splitext(transcript_file)[0]
            relative_subdir = os.path.relpath(root, TRANSCRIPT_DIR)
            print(f"\n--- Processing: {transcript_file} ---")

            match = next(
                (video for video in video_lookup if sanitize_filename(os.path.basename(video)) in sanitize_filename(base_name)),
                None
            )
            if not match:
                print(f"No matching video found for: {transcript_file}")
                # Still copy the transcript to the final directory for text-only processing
                with open(transcript_path, "r") as f:
                    transcript_text = f.read()
                
                output_dir = os.path.join(FINAL_TRANSCRIPT_DIR, relative_subdir)
                os.makedirs(output_dir, exist_ok=True)
                output_transcript_path = os.path.join(output_dir, transcript_file)
                with open(output_transcript_path, "w") as out_f:
                    out_f.write(transcript_text)
                print(f"Copied transcript to final directory for text-only processing: {output_transcript_path}")
                continue

            video_s3_uri = video_lookup[match]["s3_uri"]
            local_video_path = os.path.join(TEMP_VIDEO_DIR, sanitize_filename(match) + ".mp4")

            if not os.path.exists(local_video_path):
                print(f"Downloading {video_s3_uri} → {local_video_path}")
                download_s3_video(video_s3_uri, local_video_path)
            else:
                print(f"Using cached video: {local_video_path}")

            with open(transcript_path, "r") as f:
                transcript_text = f.read()
            print(f"Loaded transcript with {len(transcript_text)} characters.")

            timestamps = extract_timestamps(transcript_text)
            print(f"Found {len(timestamps)} timestamps.")

            frame_output_dir = os.path.join(OUTPUT_DIR, relative_subdir)
            os.makedirs(frame_output_dir, exist_ok=True)
            print(f"Extracting frames to {frame_output_dir}")

            frame_data = extract_frames(
                video_path=local_video_path,
                timestamps=timestamps,
                frame_output_dir=frame_output_dir,
                s3_bucket=BUCKET_NAME,
                video_filename=match
            )

            print(f"Extracted {len(frame_data)} frames with UUIDs.")

            mapping[match] = frame_data
            timestamp_to_uuid = {entry['timestamp']: entry['uuid'] for entry in frame_data}
            modified_transcript = inject_image_tags(transcript_text, timestamp_to_uuid)

            output_dir = os.path.join(FINAL_TRANSCRIPT_DIR, relative_subdir)
            os.makedirs(output_dir, exist_ok=True)
            output_transcript_path = os.path.join(output_dir, transcript_file)
            with open(output_transcript_path, "w") as out_f:
                out_f.write(modified_transcript)
            print(f"Saved modified transcript to {output_transcript_path}")

    with open("mapping.json", "w") as f:
        json.dump(mapping, f, indent=2)
    print("\nDone! Mapping saved to mapping.json.")


if __name__ == "__main__":
    main()