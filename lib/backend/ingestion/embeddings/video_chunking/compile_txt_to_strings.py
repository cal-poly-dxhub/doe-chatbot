import os
import sys
# turns txt files to strings 
def compile_transcripts(folder_path, video_name):
    combined_text = ""

    for root, _, files in os.walk(folder_path):
        for file in sorted(files):
            if file.endswith('.txt') and video_name in file:
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    combined_text += f.read().strip() + "\n\n"

    return combined_text.strip()

if __name__ == "__main__":
    import sys

    # final processed transcripts folder
    folder_path = sys.argv[1]

    # name of mp4 that we are trying to access
    video_name = sys.argv[2]

    transcript = compile_transcripts(folder_path, video_name)
    print(f"\n===== Transcript for '{video_name}' =====\n{transcript}")
