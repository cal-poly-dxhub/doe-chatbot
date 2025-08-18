# 3.7 initial pass, 3.5 recursive passes
# folders: mod_transcript(input)
import boto3
import json
import os
import time
from typing import List
from botocore.exceptions import ClientError
import re
import textwrap
import sys

# Import config for AWS client configuration
try:
    from . import config
except ImportError:
    # For standalone execution
    import config

def busy_wait_delay(duration_seconds=90):
    """
    Perform busy waiting for the specified duration to prevent ECS timeout
    while dealing with Claude 3.7 throttling. Default is 90 seconds (1.5 minutes).
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

def read_transcript(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error reading transcript file: {e}")
        return []

def format_transcript_for_prompt(transcript, filename=None):
    formatted_transcript = []
    for segment in transcript:
        formatted_transcript.append(
            f"[{segment['id']}] {segment['speaker']}: {segment['text']}\n"
        )
    return "".join(formatted_transcript)

def extract_section_ranges(analysis_text):
    segment_ranges = []
    lines = analysis_text.splitlines()
    for line in lines:
        if "**Segment Range:**" in line:
            matches = re.findall(r"\[seg_(\d+)(?:-(\d+))?\]", line)
            for start, end in matches:
                start = int(start)
                end = int(end) if end else start
                segment_ranges.append((start, end))
    return sorted(set(segment_ranges))


def segment_by_analysis_ranges(transcript, segment_ranges):
    id_to_segment = {int(seg['id'].split('_')[1]): seg for seg in transcript}
    chunks = []
    for start, end in segment_ranges:
        chunk = []
        for i in range(start, end + 1):
            if i in id_to_segment:
                chunk.append(id_to_segment[i])
        if chunk:
            chunks.append(chunk)
    return chunks

def analyze_transcript(transcript, filename, model_id='us.anthropic.claude-3-7-sonnet-20250219-v1:0'):
    bedrock_runtime = config.get_bedrock_runtime_client()

    # ---- 1. Format the transcript ----
    formatted_transcript = format_transcript_for_prompt(transcript, filename)

    # ---- 2. Split into chunks ----
    max_chars = 6000  # ~2500 tokens, safe margin
    transcript_chunks = textwrap.wrap(formatted_transcript, width=max_chars, break_long_words=False, break_on_hyphens=False)

    # ---- 3. Final instruction (your full prompt2) ----
    final_instruction = f"""
You are processing a transcript of a training video about [CLIENT] — an Accessible Tool for Learning About Students, used to manage special education cases from referral to placement.

Your goal is to extract **all key steps, actions, observations, and system behaviors described in the video**, and map each one to its **exact segment ID or range.**

**Organization:**

- When the transcript covers **distinct topics or functions**, you must group the content into **separate numbered sections** using this format:

    ## Section X: [Short Title]  
    **Segment Range:** [seg_start-end]

- If the transcript does not shift topics, still create at least one `## Section` using the format above.
- If any guide includes multiple topics or actions, break it into multiple `## Section` blocks.

- Every `## Section` must include a corresponding `**Segment Range:** [seg_start-end]` line directly beneath it.

**Within each section:**

- Write a **step-by-step instructional walkthrough**, and for **each step or action:**
    - Provide a **detailed, clear instructions or explanation** (e.g., " After clicking sign-in, you'll receive a text message with a 6-digit code ").
    - Assign the **exact segment ID or ID range** (e.g., [seg_1-2]).
    - Even if steps overlap in the same segments, include their **own mapped ranges.**

- If there are **system behaviors, tips, or discussions** that are important but **not explicit user steps,** include them in a "**Tips and System Behavior Notes**" section inside the same section, with their own **bullet points and segment mappings.**

**Important rules:**

- Ensure ALL sections get covered
- **Do NOT group all steps under a single segment range.** Each step or point **must have its own segment mapping.**
- **Do NOT invent or assume steps or features**—everything must be grounded in the transcript.
- **Do NOT add any additional commentary like "Based on the provided transcript, I'll create a structured guide focusing on the training session organization." or Notes.
- **If a step spans the same segment as another, overlapping segments are fine.**
- **If your section ranges do not cover the final segments of the transcript, be sure to add a final catch-all section that captures any remaining content from the last segment you used up to the last segment of the transcript.**

---

The goal is to produce a chatbot-ready output that:

- Splits instructions by topic (sections) when needed
- Includes precise, segment-level mapping for every step or note

** EXAMPLE OF GOOD OUTPUT **

# Guide: Navigating [CLIENT] System
**Segment Range:** [seg_0-380]

## Section 1: Logging into [CLIENT SOFTWARE]
**Segment Range:** [seg_46-55]

### Step-by-Step Instructions:

1. **Access the [CLIENT SOFTWARE] training site** [seg_49-50]
- Use the provided training link to access the system [seg_49]
- Note that the training site contains practice data, not real student information [seg_50]

2. **Enter your credentials** [seg_52]
- Enter your username/email and password on the sign-in page [seg_52]
- Click the sign-in button [seg_52]

3. **Complete multi-factor authentication (MFA)** [seg_53-54]
- After clicking sign-in, you'll receive a text message with a 6-digit code [seg_53]
- Enter the 6-digit code in the authentication field [seg_54]
- Click sign-in to complete the login process [seg_54]
"""

    # ---- 4. Build full message list ----
    messages = [{"role": "user", "content": chunk} for chunk in transcript_chunks]
    messages.append({"role": "user", "content": final_instruction})

    # ---- 5. Call Claude via Bedrock ----
    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 8000,
        "temperature": 0.2,
        "messages": messages
    }
    
    try:
        print(f"{model_id} Invoking with {len(messages)} messages...")
        
        # Add busy waiting delay before Claude 3.7 call to handle throttling
        busy_wait_delay(90)  # 1.5 minutes busy wait
        
        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            contentType='application/json',
            accept='application/json',
            body=json.dumps(request_body)
        )

        # ---- 6. Handle and parse response safely ----
        response_body = json.loads(response['body'].read())

        # Optional: debug print the raw response
        # print(json.dumps(response_body, indent=2))

        raw_content = response_body.get("content", [])
        result = raw_content[0].get("text", "").strip() if raw_content else ""

        if not result:
            raise ValueError("Claude returned an empty response or malformed output.")

        # ---- 7. Save the initial response to disk ----
        os.makedirs("outputs_initial", exist_ok=True)
        output_path = os.path.join("outputs_initial", f"{filename}_initial_analysis.txt")
        with open(output_path, "w") as f:
            f.write(result)

        print(f"Saved initial analysis to {output_path}")
        return result

    except Exception as e:
        print(f"Claude invocation failed: {e}")
        return f"Error invoking Claude: {e}"

def analyze_transcript_original(transcript, filename, model_id='us.anthropic.claude-3-7-sonnet-20250219-v1:0'):
    bedrock_runtime = config.get_bedrock_runtime_client()
    formatted_transcript = format_transcript_for_prompt(transcript, filename)
    #keeps throttling so defaulting to 3.5 for now
    model_id="us.anthropic.claude-3-7-sonnet-20250219-v1:0"
    # used with the 3.7 initial pass, then 3.5
    prompt = f"""
        You are processing a transcript of a training video about [CLIENT] — an Accessible Tool for Learning About Students, used to manage special education cases from referral to placement.

        Your goal is to extract **all key steps, actions, observations, and system behaviors described in the video**, and map each one to its **exact segment ID or range.**

        **Organization:**

        - When the transcript covers **distinct topics or functions**, group the content into **separate guides** (e.g., "Guide 1: Navigating the Dashboard", "Guide 2: Managing Additional Assessment Requests").
        - If the transcript **does not shift topics**, keep everything in one guide.

        **Within each guide:**
        
        - Write a **step-by-step instructional walkthrough**, and for **each step or action:**
            - Provide a **clear and concise instruction or explanation** (e.g., "Click on the 'EI transition' icon").
            - Assign the **exact segment ID or ID range** (e.g., [seg_1-2]).
            - Even if steps overlap in the same segments, include their **own mapped ranges.**

        - If there are **system behaviors, tips, or discussions** that are important but **not explicit user steps,** include them in a "**Tips and System Behavior Notes**" section inside the same guide, with their own **bullet points and segment mappings.**

        **Important rules:**

        - **Do NOT group all steps under a single segment range.** Each step or point **must have its own segment mapping.**
        - **Do NOT invent or assume steps or features**—everything must be grounded in the transcript.
        - **If a step spans the same segment as another, overlapping segments are fine.**

        ---

        The goal is to produce a chatbot-ready output that:

        - Splits instructions by topic (guides) when needed
        - Includes precise, segment-level mapping for every step or note

        TRANSCRIPT:
        {formatted_transcript}

        ---

        **Output Format Example:**
    
        ## Guide 2: Managing Additional Assessment Requests in [CLIENT SOFTWARE]
        **Segment Range:** [seg_18-39]

        ### Step-by-Step Instructions:

        1. **Understanding PWN for Additional Assessments** [seg_18-22]
        - Note that the Prior Written Notice (PWN) for additional assessments is not time-bound in [CLIENT SOFTWARE] [seg_18-20]
        - The system allows flexibility in when this step is completed [seg_20-22]

        2. **Reviewing Additional Assessment Requests** [seg_24-26]
        - Review parent requests for additional evaluations (e.g., speech, audiological) [seg_24-25]
        - Make determination to approve or reject based on your agency's policy [seg_25-26]

        3. **Timeline for Responding to Additional Assessment Requests** [seg_30-33]
        - CPSEs have 2-3 days to review and respond to additional assessment requests [seg_30-31]
        - Note that core evaluations will continue to move forward regardless of additional assessment status [seg_30-33]

        ### Tips and System Behavior Notes:
        - Core evaluations will proceed regardless of whether additional assessment requests have been processed [seg_30-32]
        - There may be a discrepancy between the system limit (2 additional assessments) and actual practice (up to 4) [seg_33-38]
        - This feature appears to be under discussion for enhancement [seg_32-33]
        """
    # this prompt is more specific (needs to be used with 3.5 to ask it to make segments
    prompt2 = f"""
        You are processing a transcript of a training video about [CLIENT] — an Accessible Tool for Learning About Students, used to manage special education cases from referral to placement.

        Your goal is to extract **all key steps, actions, observations, and system behaviors described in the video**, and map each one to its **exact segment ID or range.**

        **Organization:**

        - When the transcript covers **distinct topics or functions**, you must group the content into **separate numbered sections** using this format:

            ## Section X: [Short Title]  
            **Segment Range:** [seg_start-end]

        - If the transcript does not shift topics, still create at least one `## Section` using the format above.
        - If any guide includes multiple topics or actions, break it into multiple `## Section` blocks.

        - Every `## Section` must include a corresponding `**Segment Range:** [seg_start-end]` line directly beneath it.

        **Within each section:**

        - Write a **step-by-step instructional walkthrough**, and for **each step or action:**
            - Provide a **detailed, clear instructions or explanation** (e.g., " After clicking sign-in, you'll receive a text message with a 6-digit code ").
            - Assign the **exact segment ID or ID range** (e.g., [seg_1-2]).
            - Even if steps overlap in the same segments, include their **own mapped ranges.**

        - If there are **system behaviors, tips, or discussions** that are important but **not explicit user steps,** include them in a "**Tips and System Behavior Notes**" section inside the same section, with their own **bullet points and segment mappings.**

        **Important rules:**

        - Ensure ALL sections get covered
        - **Do NOT group all steps under a single segment range.** Each step or point **must have its own segment mapping.**
        - **Do NOT invent or assume steps or features**—everything must be grounded in the transcript.
        - **Do NOT add any additional commentary like "Based on the provided transcript, I'll create a structured guide focusing on the training session organization." or Notes.
        - **If a step spans the same segment as another, overlapping segments are fine.**
        - **If your section ranges do not cover the final segments of the transcript, be sure to add a final catch-all section that captures any remaining content from the last segment you used up to the last segment of the transcript.**

        ---

        The goal is to produce a chatbot-ready output that:

        - Splits instructions by topic (sections) when needed
        - Includes precise, segment-level mapping for every step or note

        ** EXAMPLE OF GOOD OUTPUT **

        # Guide: Navigating [CLIENT] System
        **Segment Range:** [seg_0-380]

        ## Section 1: Logging into [CLIENT SOFTWARE]
        **Segment Range:** [seg_46-55]

        ### Step-by-Step Instructions:

        1. **Access the [CLIENT SOFTWARE] training site** [seg_49-50]
        - Use the provided training link to access the system [seg_49]
        - Note that the training site contains practice data, not real student information [seg_50]

        2. **Enter your credentials** [seg_52]
        - Enter your username/email and password on the sign-in page [seg_52]
        - Click the sign-in button [seg_52]

        3. **Complete multi-factor authentication (MFA)** [seg_53-54]
        - After clicking sign-in, you'll receive a text message with a 6-digit code [seg_53]
        - Enter the 6-digit code in the authentication field [seg_54]
        - Click sign-in to complete the login process [seg_54]

        TRANSCRIPT:
        {formatted_transcript}
    """

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 8000,
        "temperature": 0.2,
        "messages": [{"role": "user", "content": prompt}]
    }

    retries = 0
    delay = 10
    max_retries = 3
    while retries <= max_retries:
        try:
            print(f"Invoking Claude with model: {model_id}...")
            
            # Add busy waiting delay before Claude 3.7 call to handle throttling
            busy_wait_delay(90)  # 1.5 minutes busy wait
            
            response = bedrock_runtime.invoke_model(
                modelId=model_id,
                contentType='application/json',
                accept='application/json',
                body=json.dumps(request_body)
            )
            print("Claude invoked successfully.")
            response_body = json.loads(response['body'].read())
            result = response_body['content'][0]['text']
            os.makedirs("outputs_initial", exist_ok=True)
            with open(os.path.join("outputs_initial", f"{filename}_initial_analysis.txt"), 'w') as f:
                f.write(result)
            return result
        except ClientError as e:
            code = e.response['Error']['Code']
            if code in ["ThrottlingException", "TooManyRequestsException", "Throttled"]:
                retries += 1
                if retries > max_retries:
                    return "Error: Max retries exceeded due to throttling."
                wait = delay * (2 ** (retries - 1))
                print(f"Throttled. Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                return f"Error analyzing transcript: {e.response['Error']['Message']}"
        except Exception as e:
            return f"Error invoking Claude: {e}"

def recursively_segment(transcript: List[dict], filename: str, max_depth: int = 1, current_depth: int = 0) -> str:
    # first pass through is with 3.7, recursive pass through following will be 3.5
    # logic below is to alter models (throttling), however currently keeping it to 3.7
    model_id = 'us.anthropic.claude-3-7-sonnet-20250219-v1:0' if current_depth == 100 else 'us.anthropic.claude-3-7-sonnet-20250219-v1:0'
    if max_depth == 0 or current_depth >= max_depth:
        return analyze_transcript(transcript, filename, model_id=model_id)

    print("Performing initial analysis to identify section segment ranges...")
    initial_analysis = analyze_transcript(transcript, filename, model_id=model_id)
    segment_ranges = extract_section_ranges(initial_analysis)
    chunks = segment_by_analysis_ranges(transcript, segment_ranges)
    print(chunks)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    combined_guides = []
    for i, chunk in enumerate(chunks):
        print(f"\n[Depth {current_depth}] Analyzing section chunk {i + 1}/{len(chunks)}")
        chunk_filename = f"{filename}_section{i+1}"
        guide = recursively_segment(chunk, chunk_filename, max_depth, current_depth + 1)

        video_output_dir = os.path.join(OUTPUT_FOLDER, filename)
        os.makedirs(video_output_dir, exist_ok=True)
        output_path = os.path.join(video_output_dir, f"{chunk_filename}_analysis.txt")
        with open(output_path, 'w') as f:
            f.write(guide)
        combined_guides.append(guide.strip())
    return "\n\n".join(combined_guides)

def save_analysis(analysis, output_file="transcript_analysis.txt"):
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w') as file:
            file.write(analysis)
        print(f"Analysis saved to {output_file}")
    except Exception as e:
        print(f"Error saving analysis: {e}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python generate_llm_summaries.py <input_folder> <output_folder>")
        sys.exit(1)

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    global OUTPUT_FOLDER
    OUTPUT_FOLDER = output_folder

    for filename in os.listdir(input_folder):
        if filename.endswith("_sequential_transcript.json"):
            file_path = os.path.join(input_folder, filename)
            print(f"Loading {file_path}")
            transcript = read_transcript(file_path)

            if not transcript:
                print("Failed to load transcript. Skipping.")
                continue

            last_segment = transcript[-1]
            video_duration_min = last_segment.get("end_time", 0) / 60

            video_base_name = filename.replace("_sequential_transcript.json", "")
            video_output_dir = os.path.join(output_folder, video_base_name)
            os.makedirs(video_output_dir, exist_ok=True)

            if video_duration_min > 45:
                print(f"Using recursive segmentation (max_depth=1) for {video_base_name}")
                analysis = recursively_segment(transcript, video_base_name, max_depth=1)
            else:
                print(f"Using single-pass Claude 3.5 guide mode for {video_base_name}")
                analysis = analyze_transcript_original(transcript, video_base_name)

                output_path = os.path.join(video_output_dir, f"{video_base_name}_analysis.txt")
                with open(output_path, "w") as f:
                    f.write(analysis)
                print(f"Single-pass guide analysis saved to {output_path}")  
                
if __name__ == "__main__":
    main()
