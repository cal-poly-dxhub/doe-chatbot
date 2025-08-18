import os
import json
import sys

def process_transcribe_output(file_path):
    with open(file_path, 'r') as file:
        transcribe_data = json.load(file)

    if ('results' not in transcribe_data or
        'speaker_labels' not in transcribe_data['results'] or
        'items' not in transcribe_data['results']):
        print("Error: Unexpected format in the transcript file.")
        return None

    if 'audio_segments' in transcribe_data['results']:
        segments = sorted(
            transcribe_data['results']['audio_segments'],
            key=lambda x: float(x['start_time'])
        )
        sequential_transcript = []
        for idx, segment in enumerate(segments):
            sequential_transcript.append({
                "id": f"seg_{idx}",
                "speaker": segment['speaker_label'],
                "text": segment['transcript'],
                "start_time": float(segment['start_time']),
                "end_time": float(segment['end_time'])
            })
        return sequential_transcript

    speaker_segments = [
        {
            'speaker_label': s['speaker_label'],
            'start_time': float(s['start_time']),
            'end_time': float(s['end_time']),
            'items': [item['start_time'] for item in s['items']]
        }
        for s in transcribe_data['results']['speaker_labels']['segments']
    ]
    speaker_segments.sort(key=lambda x: x['start_time'])

    items_dict = {}
    for item in transcribe_data['results']['items']:
        if 'id' in item and 'alternatives' in item and len(item['alternatives']) > 0:
            if item['type'] == 'pronunciation':
                items_dict[int(item['id'])] = {
                    'content': item['alternatives'][0]['content'],
                    'type': item['type'],
                    'start_time': float(item.get('start_time', '0')),
                    'end_time': float(item.get('end_time', '0')),
                    'speaker_label': item.get('speaker_label', None)
                }
            else:
                items_dict[int(item['id'])] = {
                    'content': item['alternatives'][0]['content'],
                    'type': item['type']
                }

    sequential_transcript = []
    for idx, segment in enumerate(speaker_segments):
        speaker = segment['speaker_label']
        segment_start = segment['start_time']
        segment_end = segment['end_time']
        segment_items = [
            (item_id, item)
            for item_id, item in items_dict.items()
            if item['type'] == 'pronunciation' and segment_start <= item['start_time'] < segment_end
        ]
        segment_items.sort(key=lambda x: x[1]['start_time'])

        segment_text = []
        for item_id, item in segment_items:
            if segment_text and item['type'] != 'punctuation':
                segment_text.append(" ")
            segment_text.append(item['content'])
            if item_id + 1 in items_dict and items_dict[item_id + 1]['type'] == 'punctuation':
                segment_text.append(items_dict[item_id + 1]['content'])

        if segment_items:
            sequential_transcript.append({
                "id": f"seg_{idx}",
                "speaker": speaker,
                "text": "".join(segment_text),
                "start_time": segment_start,
                "end_time": segment_end
            })

    return sequential_transcript

def format_for_reading(sequential_transcript):
    readable_text = [
        f"[{entry['id']}] {entry['speaker']}: {entry['text']}"
        for entry in sequential_transcript
    ]
    lookup_map = {entry['id']: entry for entry in sequential_transcript}
    return {
        "text": "\n".join(readable_text),
        "structured": sequential_transcript,
        "lookup": lookup_map
    }

def main():
    if len(sys.argv) != 3:
        print("Usage: python format_transcripts.py <INPUT_FOLDER> <OUTPUT_FOLDER>")
        sys.exit(1)

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(input_folder):
        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(input_folder, filename)
        print(f"Processing {filename}...")

        base_name = os.path.splitext(filename)[0]
        sequential_transcript = process_transcribe_output(file_path)

        if sequential_transcript:
            formatted_output = format_for_reading(sequential_transcript)

            with open(f"{output_folder}/{base_name}_sequential_transcript.json", 'w') as f:
                json.dump(sequential_transcript, f, indent=2)

            with open(f"{output_folder}/{base_name}_readable_transcript.txt", 'w') as f:
                f.write(formatted_output["text"])

            with open(f"{output_folder}/{base_name}_transcript_lookup.json", 'w') as f:
                json.dump(formatted_output["lookup"], f, indent=2)

            print(f"Saved: {base_name}_sequential_transcript.json, _readable_transcript.txt, _lookup.json")

if __name__ == "__main__":
    main()
