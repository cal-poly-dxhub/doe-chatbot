import datetime
import re
import os, shutil
import json
import markdown
from datetime import timedelta
import sys


def format_timestamp(seconds):
    """Convert seconds to HH:MM:SS format for YouTube timestamps"""
    return str(int(seconds))

def replace_segments_with_timestamps(text, segments_data):
    """Replace [seg_X], [seg_X-Y], and [seg_X, seg_Y] with (!?#Timestamp:<start_time>)"""
    
    segment_map = {s["id"]: s["start_time"] for s in segments_data}

    # handles ranges of segments like [seg_4-6]
    def range_replacer(match):
        start_seg = match.group(1)
        start_id = f"seg_{start_seg}"
        start_time = segment_map.get(start_id)
        return f"(!?#Timestamp:{start_time})" if start_time is not None else match.group(0)

    text = re.sub(r'\[seg_(\d+)-(\d+)\]', range_replacer, text)

    # handles lists of segments like [seg_8, seg_13] - returning the first timestamp only
    def list_replacer(match):
        # Extract full content like "[seg_1, seg_13, seg_19-20]"
        content = match.group(0)

        # Match the first segment or range like seg_1 or seg_19-20
        first = re.search(r'seg_(\d+)(?:-\d+)?', content)
        if not first:
            return content

        first_seg_id = f"seg_{first.group(1)}"
        start_time = segment_map.get(first_seg_id)

        if start_time is not None:
            return f"(!?#Timestamp:{start_time})"
        else:
            return content  # fallback to original if not found

    text = re.sub(r'\[(?:seg_\d+(?:-\d+)?(?:,\s*seg_\d+(?:-\d+)?)*?)\]', list_replacer, text)

    # handles single segments like [seg_5]
    def single_replacer(match):
        seg_id = f"seg_{match.group(1)}"
        start_time = segment_map.get(seg_id)
        return f"(!?#Timestamp:{start_time})" if start_time is not None else match.group(0)

    text = re.sub(r'\[seg_(\d+)\]', single_replacer, text)

    return text

def prepare_sequential_only_dir(source_dir, target_dir):
    """prepares a directory of _sequential_transcript.json files for timestamp processing"""
    os.makedirs(target_dir, exist_ok=True)
    for filename in os.listdir(source_dir):
        if filename.endswith("_sequential_transcript.json"):
            src = os.path.join(source_dir, filename)
            dst = os.path.join(target_dir, filename)
            shutil.copyfile(src, dst)
            print(f"Copied: {filename} to {target_dir}/")

def main():
    if len(sys.argv) != 5:
        print("Usage: python insert_timestamps.py <summary_input_folder> <seq_json_input_folder> <seq_json_output_folder> <timestamp_output_root>")
        sys.exit(1)

    summary_input_folder = sys.argv[1]
    seq_json_input_folder = sys.argv[2]
    seq_json_output_folder = sys.argv[3]
    timestamp_output_root = sys.argv[4]

    prepare_sequential_only_dir(seq_json_input_folder, seq_json_output_folder)

    summary_files = []
    for root, dirs, files in os.walk(summary_input_folder):
        for f in files:
            if f.endswith(".txt"):
                summary_files.append(os.path.join(root, f))

    print("summary files \n")
    print(summary_files)

    for summary_path in summary_files:
        filename_only = os.path.basename(summary_path)

        if "_section" in filename_only:
            base_name = filename_only.split("_section")[0]
        elif filename_only.endswith("_analysis.txt"):
            base_name = filename_only.replace("_analysis.txt", "")
        else:
            print(f"Skipping unexpected file format: {filename_only}")
            continue

        transcript_path = os.path.join(seq_json_output_folder, f"{base_name}_sequential_transcript.json")
        if not os.path.exists(transcript_path):
            print(f"Transcript not found for {summary_path}, skipping.")
            continue

        try:
            with open(summary_path, "r", encoding="utf-8") as file:
                analysis_text = file.read()
        except FileNotFoundError:
            print(f"Error: {summary_path} not found.")
            continue

        try:
            with open(transcript_path, "r", encoding="utf-8") as file:
                segments_data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"Error: {transcript_path} is not valid.")
            continue

        updated_text = replace_segments_with_timestamps(analysis_text, segments_data)
        video_subdir = os.path.basename(os.path.dirname(summary_path))
        output_dir = os.path.join(timestamp_output_root, video_subdir)
        os.makedirs(output_dir, exist_ok=True)

        output_filename = f"{filename_only.replace('.txt', '')}_timestamps.txt"
        output_path = os.path.join(output_dir, output_filename)

        with open(output_path, "w", encoding="utf-8") as out_file:
            out_file.write(updated_text)

        print(f"Saved timestamped output to {output_path}")
if __name__ == "__main__":
    main()
