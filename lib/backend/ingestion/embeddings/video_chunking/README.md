## Video Segmentation + Processing Workflow

This branch processes video transcripts to produce timestamped, formatted helpdesk-style documentation enriched with media from videos/Zoom recordings.

---

### 1. Transcribe Videos
Use `transcribe.py` to initiate transcription jobs from an S3 bucket containing your video files.  
This sends each `.mp4` to Amazon Transcribe and creates JSON outputs in your designated S3 output bucket.

```bash
python3 transcribe.py <vid_input_bucket> <vid_object_key> <transcribe_output_bucket> <transcribe_output_folder><vid_to_s3_link_mapping>
```
---

### 2. Download Transcribe Output Files

Download the `.json` outputs from your S3 bucket into a local directory:

```bash
aws s3 cp s3://your-transcribe-output-bucket/output_folder/ local_transcripts/ --recursive
```

---

### 3. Rename Transcribe Job-Named Transcripts

Use `rename_job_names.py` to convert Amazon Transcribe’s job-named outputs back to the original video filenames using the `vid_name_map.json`:

```bash
python3 rename_job_names.py vid_name_map.json local_transcripts/
```

---

### 4. Format Transcripts into Human-Readable Files

Use `format_transcript.py` to convert raw `.json` transcript files into readable formats:

```bash
python3 format_transcript.py <input_transcript_dir> <output_dir>
```

Outputs for each file:

* `*_sequential_transcript.json`: speaker-labeled structured segments
* `*_readable_transcript.txt`: readable plain-text transcript
* `*_transcript_lookup.json`: maps segment IDs to content

---

### 5. Generate Recursive/Regular Segment Summaries

Use `generate_llm_summaries.py` to create helpdesk-style summaries from the sequential transcripts:

```bash
python3 generate_llm_summaries.py <input_dir> <output_dir>
```

Also generates an intermediate folder (`output_initial`) with raw LLM responses.

---

### 6. Insert Timestamps into Summaries

Use `insert_timestamps.py` to replace `[seg_5]`, `[seg_4-6]`, etc. with real timestamps like `(!?#Timestamp:123.45)`:

```bash
python3 insert_timestamps.py <summaries_dir> <transcript_dir> sequential_transcripts_only <output_dir>
```

---

### 7. Extract Snapshot Frames from Video + Insert UUIDs

Use `frame_extraction.py` to pull frames from videos at referenced timestamps, checks if the images are relevant store the frames in S3, insert `(!?#Image:<uuid>)` tags, and save the mappings:

```bash
python3 frame_extraction.py <timestamped_transcripts_dir> <frame_output_dir> <final_output_dir> <vid_name_map.json> <video_temp_dir> <dry_run:true|false>
```
This also removes irrelvant images using nova pro.
---

### 8. Inject Top-Level Video Links

Use `insert_vid_links.py` to assign each transcript a UUID-mapped video link (reused across transcripts) and insert a `(!?#Video:<uuid>)` tag at the top:

```bash
python3 insert_vid_links.py <final_transcript_dir> <output_dir> <video_bucket> <dry_run:true|false>
```

This generates `video_uuid_mapping.json`.

---

### 9. Convert txt files to strings

Use `compile_txt_to_strings.py` to get list of strings from the formatted transcript txts.

```bash
python3 compile_txt_to_strings.py <final_transcript_folder> <video_object_key> 

```
---

### (Optional) Clean Up DynamoDB Entries

Use `dynamo_delete.py` to delete media UUIDs from the DynamoDB table:

```bash
python3 dynamo_delete.py <media-type>
```

---


### (Optional) Run Full Pipeline with Bash Script

Use `video_pipeline.sh` to execute the full workflow from transcription to final transcript generation with images and UUIDs:

```bash
bash run_pipeline.sh
```


---

## Summary of File Purposes

| File                        | Purpose                                                             |
| --------------------------- | ------------------------------------------------------------------- |
| `transcribe.py`             | Sends `.mp4` files to Amazon Transcribe and logs job metadata       |
| `rename_job_names.py`       | Renames Amazon Transcribe output files using `vid_name_map.json`    |
| `format_transcript.py`      | Converts Transcribe JSON to readable and structured formats         |
| `generate_llm_summaries.py` | Creates helpdesk-style segment summaries (recursively or regularly) |
| `insert_timestamps.py`      | Replaces segment IDs in summaries with real video timestamps        |
| `frame_extraction.py`       | Pulls video frames, uploads them to S3, injects `(!?#Image:<uuid>)` |
| `insert_vid_links.py`       | Assigns top-level `(!?#Video:<uuid>)` links to transcripts          |
| `dynamo_delete.py`          | Deletes UUID mappings for a given media type in DynamoDB            |
| `video_pipeline.sh`         | Shell script to run the full workflow end-to-end                    |

```
```
