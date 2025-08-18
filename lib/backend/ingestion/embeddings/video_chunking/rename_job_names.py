import os
import json
import sys

def load_reverse_mapping(mapping_file):
    with open(mapping_file, "r") as f:
        job_map = json.load(f)
    return {v["job_name"]: k for k, v in job_map.items()}

def rename_transcript_files(mapping_file, transcript_dir):
    reverse_map = load_reverse_mapping(mapping_file)

    for fname in os.listdir(transcript_dir):
        if not fname.endswith(".json"):
            continue

        job_name = os.path.splitext(fname)[0]

        if job_name not in reverse_map:
            print(f"[SKIP] No mapping found for job name: {job_name}")
            continue

        original_filename = reverse_map[job_name]
        safe_filename = original_filename.replace("/", "_")
        old_path = os.path.join(transcript_dir, fname)
        new_path = os.path.join(transcript_dir, safe_filename + ".json")

        if os.path.exists(new_path):
            print(f"[SKIP] File already exists: {new_path}")
            continue

        os.rename(old_path, new_path)
        print(f"[RENAMED] {fname} → {safe_filename}.json")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python rename_job_names.py <mapping_file> <transcript_dir>")
        sys.exit(1)

    mapping_file = sys.argv[1]
    transcript_dir = sys.argv[2]
    rename_transcript_files(mapping_file, transcript_dir)
