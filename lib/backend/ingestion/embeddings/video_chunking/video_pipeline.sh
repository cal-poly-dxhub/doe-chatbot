#!/bin/bash

set -e  # Exit on error
set -o pipefail
set -u  # Treat unset variables as errors

# === CONFIG ===
TRANSCRIBE_OUTPUT_BUCKET="video-transcriptions"
TRANSCRIBE_OUTPUT_FOLDER="new_transcripts_jun21"

VID_INPUT_BUCKET="full-vids-bucket"
VID_OBJECT_KEY="1.3 Video - How to View Students Assigned to My Caseload.mp4"

#job_name_map.json (what i alr have locally)
VID_TO_S3_LINK_MAPPING="video_name_map_jun21.json"
LOCAL_TRANSCRIPT_DIR="transcripts_jun21"
VIDEO_BUCKET="full-vids-bucket"
FORMATTED_TRANSCRIPTS_FOLDER="modified_transcripts_jun21"
GENERATED_SUMMARIES_FOLDER="generated_summaries_jun21"
TIMESTAMPED_TRANSCRIPTS_FOLDER="timestamped_ouputs_jun21"
FINAL_TRANSCRIPTS_FOLDER="final_jun21"
FRAMES_OUTPUT_FOLDER="frames_jun21"
VID_LINKS_FINAL_TRANSCRIPTS_FOLDER="vid_links_final_jun21"
DRY_RUN_FLAG=true 

# === STEP 1: Transcribe videos (assumes transcribe.py handles the S3 bucket directly) ===
echo "Step 1: Running transcription job on S3 bucket..."
python3 transcribe.py \
  "${VID_INPUT_BUCKET}"  \
  "${VID_OBJECT_KEY}" \
  "${TRANSCRIBE_OUTPUT_BUCKET}"\
  "${TRANSCRIBE_OUTPUT_FOLDER}/" \
  "${VID_TO_S3_LINK_MAPPING}"

# === STEP 2: Download transcripts ===
mkdir -p "${LOCAL_TRANSCRIPT_DIR}"
echo "Step 2: Downloading transcripts from S3..."
aws s3 cp "s3://${TRANSCRIBE_OUTPUT_BUCKET}/${TRANSCRIBE_OUTPUT_FOLDER}/" "${LOCAL_TRANSCRIPT_DIR}/" --recursive

# === STEP 3: Rename transcripts back to original video names ===
echo "Step 3: Renaming job-named transcripts to original video names..."
# args: mapping file, local transcripts folder
python3 rename_job_names.py "${VID_TO_S3_LINK_MAPPING}" "${LOCAL_TRANSCRIPT_DIR}"

# === STEP 4: Format transcripts ===
echo "Step 4: Formatting transcripts..."
# args: local transcripts folder, modifed transcripts folder
python3 format_transcript.py "${LOCAL_TRANSCRIPT_DIR}" "${FORMATTED_TRANSCRIPTS_FOLDER}"

# === STEP 5: Generate recursive/non-recursive summaries ===
echo "Step 5: Generating recursive segment summaries..."
# args: modified_transcripts folder, generated summaries folder
python3 generate_llm_summaries.py "${FORMATTED_TRANSCRIPTS_FOLDER}" "${GENERATED_SUMMARIES_FOLDER}"
# this step also generates an output_initial folder to see the first pass

# === STEP 6: Insert timestamps into summaries ===
echo "Step 6: Inserting timestamps..."
python3 insert_timestamps.py "${GENERATED_SUMMARIES_FOLDER}" "${FORMATTED_TRANSCRIPTS_FOLDER}" sequential_transcripts_only "${TIMESTAMPED_TRANSCRIPTS_FOLDER}"

# === STEP 7: Extract images + inject UUIDs + save mapping ===
echo "Step 7: Extracting images from videos and inserting UUIDs..."
# last argument is for running a dry run (true), toggle it off if want to add uuids
# currently named the media type as video-image-test, need to change later
python3 frame_extraction.py "${TIMESTAMPED_TRANSCRIPTS_FOLDER}" "${FRAMES_OUTPUT_FOLDER}" "${FINAL_TRANSCRIPTS_FOLDER}" "${VID_TO_S3_LINK_MAPPING}" temp_videos2 false
# output: mappings.json, mappings from timestamps to specific frames stored in s3

# === STEP 8: Inject top-level video links into final transcripts ===
echo "Step 8: Injecting top-level video UUIDs into transcripts..."
python3 insert_vid_links.py "${FINAL_TRANSCRIPTS_FOLDER}" "${VID_LINKS_FINAL_TRANSCRIPTS_FOLDER}" "${VIDEO_BUCKET}" true
# outputs a vid_name_map json

# === STEP 9: Compile all .txt files per video into list of strings ===
echo "Step 9: Compiling final transcripts into list of strings..."
python3 compile_txt_to_strings.py "${VID_LINKS_FINAL_TRANSCRIPTS_FOLDER}" "${VID_OBJECT_KEY}"

# echo "All steps completed successfully."
