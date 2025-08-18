# not used in workflow right now, but this is the non-recursive approach to generate summaries.
import boto3
import json
import os
import time
from botocore.exceptions import ClientError

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
    """Read the sequential transcript JSON file and return its contents."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error reading transcript file: {e}")
        return []

def format_transcript_for_prompt(transcript, filename=None):
    """Format the transcript in a way that's suitable for the prompt and optionally save it."""
    formatted_transcript = []
    for segment in transcript:
        formatted_transcript.append(
            f"[{segment['id']}] {segment['speaker']}: {segment['text']}\n"
        )
    result = "".join(formatted_transcript)
    return result

def analyze_transcript(transcript, filename):
    """
    Use Claude to analyze the transcript for votes, topic transitions,
    and connections to agenda items.
    """
    # Initialize the Bedrock Runtime client
    bedrock_runtime = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-west-2'
    )

    # Format the transcript for the prompt
    formatted_transcript = format_transcript_for_prompt(transcript, filename)
    prompt = f"""
        You are processing a transcript of a training video about [CLIENT SOFTWARE].

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

    # ouptut the prompt
    # os.makedirs("new_outputs/prompts", exist_ok=True)
    # output_filename = os.path.splitext(filename)[0] + ".txt"
    # with open(os.path.join("new_outputs/prompts", output_filename), "w") as f:
    #     f.write(prompt)

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 8000,  # Increased for comprehensive analysis
        "temperature": 0.2,  # Lower temperature for more focused analysis
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }

    # ========== streaming implementation ================
    # try:
    #     # Make the streaming API call
    #     print("Invoking Claude with streaming...")
    #     response = bedrock_runtime.invoke_model_with_response_stream(
    #         modelId='us.anthropic.claude-3-7-sonnet-20250219-v1:0',
    #         contentType='application/json',
    #         accept='application/json',
    #         body=json.dumps(request_body)
    #     )

    #     # Process the streaming response
    #     analysis_chunks = []
    #     print("\nStreaming response:")
    #     print("-" * 80)

    #     # Iterate through the streaming chunks
    #     for event in response.get('body'):
    #         # Process each chunk
    #         if 'chunk' in event:
    #             chunk_data = json.loads(event['chunk']['bytes'])
    #             if chunk_data.get('type') == 'content_block_delta' and chunk_data.get('delta', {}).get('text'):
    #                 text_chunk = chunk_data['delta']['text']
    #                 print(text_chunk, end='', flush=True)
    #                 analysis_chunks.append(text_chunk)

    #     print("\n" + "-" * 80)

    #     # Combine all chunks to return the complete analysis
    #     analysis = ''.join(analysis_chunks)
    #     return analysis

    # ==== non-streaming implementation =======
    retries = 0
    delay = 10  # start with 10 seconds (adjust if needed)
    max_retries = 3
    while retries <= max_retries:
        try:
            # Make the API call
            print("Invoking Claude...")
            
            # Add busy waiting delay before Claude 3.7 call to handle throttling
            busy_wait_delay(90)  # 1.5 minutes busy wait
            
            response = bedrock_runtime.invoke_model(
                modelId='us.anthropic.claude-3-7-sonnet-20250219-v1:0',
                contentType='application/json',
                accept='application/json',
                body=json.dumps(request_body)
            )
            print("Claude invoked successfully.")
            # Parse the response
            response_body = json.loads(response['body'].read())

            # Get the response text
            analysis = response_body['content'][0]['text']

            return analysis
        
        # retry if throttling encountered
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"Error invoking Claude: {error_code} - {error_message}")

            if error_code in ["ThrottlingException", "TooManyRequestsException", "Throttled"]:
                retries += 1
                if retries > max_retries:
                    print("Max retries exceeded. Giving up.")
                    return f"Error: Max retries exceeded due to throttling."
                wait_time = delay * (2 ** (retries - 1))  # exponential backoff
                print(f"Throttled. Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(wait_time)
            else:
                # not a throttling issue, break immediately
                return f"Error analyzing transcript: {error_message}"

        except Exception as e:
            print(f"Error invoking Claude: {e}")
            return f"Error analyzing transcript: {e}"

def save_analysis(analysis, output_file="transcript_analysis.txt"):
    """Save the analysis to a file."""
    try:
        with open(output_file, 'w') as file:
            file.write(analysis)
        print(f"Analysis saved to {output_file}")
    except Exception as e:
        print(f"Error saving analysis: {e}")

def main():
    transcript_dir = "mod_transcripts_recursive/"
    for filename in os.listdir(transcript_dir):
        if filename.endswith("_sequential_transcript.json"):
            file_path = os.path.join(transcript_dir, filename)
            print(file_path)
            transcript = read_transcript(file_path)
            print(f"Processing {filename}...")

            if not transcript:
                print("Failed to load transcript. Exiting.")
                return

            print("Analyzing transcript...")

            # Analyze the transcript
            analysis = analyze_transcript(transcript, filename)

            # Print the analysis
            print("\nANALYSIS RESULTS:")
            print("-" * 80)
            print(analysis)
            print("-" * 80)

            # Save the analysis to a file
            os.makedirs("outputs_REAL", exist_ok=True)
            output_filename = os.path.splitext(filename)[0] + ".txt"
            save_analysis(analysis, os.path.join("outputs", output_filename))

            print("\nAnalysis complete!")

if __name__ == "__main__":
    main()
