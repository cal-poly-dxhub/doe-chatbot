import json
import sys
# this file allows you to enter a specific segment # to see the info at that segment.
# need to manually enter filepath of *_sequential_transcript.json and *_transcript_lookup.json

def main():

    if len(sys.argv) < 2:
        print("Usage: python3 lookup.py <segment_num>")
        exit()
    chunk_num = sys.argv[1]

    with open('mod_transcripts/EI_Transition_EI_Transition_Demo-74ef10f5-3f58-40ce-84d3-d1c0dd9d6653_sequential_transcript.json', 'r') as file:
        transcript = json.load(file)

    # Reference by index
    first_segment = transcript[0]
    print(f"First segment: {first_segment['speaker']}: {first_segment['text']}")

    # Reference by ID using the lookup dictionary
    with open('mod_transcripts/EI_Transition_EI_Transition_Demo-74ef10f5-3f58-40ce-84d3-d1c0dd9d6653_transcript_lookup.json', 'r') as file:
        lookup = json.load(file)

    # lookup specific segment 
    segment = lookup[f'seg_{chunk_num}'] 
    print(f"Segment {segment['id']}: {segment['speaker']}: {segment['text']}")

if __name__ == "__main__":
    main()