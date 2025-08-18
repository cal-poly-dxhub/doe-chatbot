import boto3
import re
import os

def create_replacement_function(replacement_template):
    """Create a function that generates the replacement string based on the template and matched URL"""
    def replace_func(match):
        # Extract the URL from the matched pattern
        url = match.group(1)
        # Replace the placeholder in the template with the actual URL
        return replacement_template.replace('{}', url)
    return replace_func

def process_file(s3_client, source_bucket, target_bucket, file_key, pattern_configs):
    """Process a single file by downloading, replacing patterns, and uploading"""
    try:
        local_file_path = "plaintexts2/" + os.path.basename(file_key)
        
        # Download file from source bucket
        print(f"Downloading {file_key} from {source_bucket}")
        s3_client.download_file(source_bucket, file_key, local_file_path)
        
        # Read the file content
        with open(local_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Track if any changes were made
        changes_made = False
        replacements_count = 0
        
        # Apply each pattern replacement
        for pattern, replacement, ending in pattern_configs:
            # Create pattern that handles the specific ending
            regex_pattern = f'{re.escape(pattern)}(.*?){re.escape(ending)}'
            
            # Create replacement function for this pattern
            replacement_func = create_replacement_function(replacement)
            
            # Count occurrences before replacement
            matches = re.findall(regex_pattern, content)
            match_count = len(matches)
            
            if match_count > 0:
                # Apply the replacement using regex
                new_content = re.sub(regex_pattern, replacement_func, content)
                
                if new_content != content:
                    changes_made = True
                    replacements_count += match_count
                    content = new_content
                    print(f"  - Replaced {match_count} instances of '{pattern}...{ending}'")
        
        # Find and wrap bare S3 bucket URLs
        s3_bucket_url = "https://scenes.s3.amazonaws.com/"
        s3_url_pattern = f"{re.escape(s3_bucket_url)}[^\\s'\",()<>\$$\$${{}}]+"
        
        # Find all bare S3 URLs
        s3_urls = re.findall(s3_url_pattern, content)
        s3_url_count = len(s3_urls)
        
        if s3_url_count > 0:
            print(f"  - Found {s3_url_count} bare S3 URLs")
            
            # Replace each bare S3 URL with the image pattern
            for url in s3_urls:
                # Only replace if the URL is not already within a pattern
                # Check if the URL is not already part of one of our patterns
                is_standalone = True
                for pattern, _, ending in pattern_configs:
                    if re.search(f'{re.escape(pattern)}.*?{re.escape(url)}.*?{re.escape(ending)}', content):
                        is_standalone = False
                        break
                
                if is_standalone:
                    # Replace with the image pattern
                    pattern_start = "(!?#Image:"
                    pattern_end = ")"
                    content = content.replace(url, f"{pattern_start}{url}{pattern_end}")
                    changes_made = True
                    replacements_count += 1
                    print(f"  - Wrapped S3 URL in image pattern: {url}")
        
        # Write the modified content back to the file
        with open(local_file_path, 'w', encoding='utf-8') as file:
            file.write(content)
        
        # Upload the modified file to target bucket
        print(f"Uploading modified {file_key} to {target_bucket}")
        s3_client.upload_file(local_file_path, target_bucket, file_key)
        
        
        if changes_made:
            print(f"Replaced {replacements_count} pattern instances in {file_key}")
        else:
            print(f"No patterns found in {file_key}")
            
        return True
            
    except Exception as e:
        print(f"Error processing {file_key}: {str(e)}")
        
        # Clean up the local file if it exists
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            
        return False

def main():
    # Predefined S3 bucket names
    source_bucket = "plaintext"  # Replace with your source bucket name
    target_bucket = "processed-plaintext"  # Replace with your target bucket name
    
    # Predefined patterns and replacements with specific endings
    # Format: [pattern, replacement_template, ending_character]
    pattern_configs = [
        ["[$Image: ", "(!?#Image:{})", "$]"],
        ["[$VIDEO_TIMESTAMP{", "(!?#Timestamp:{})", "}]"],
        ["[Video Link: ", "(!?#Video:{})", "]"]
    ]
    
    print(f"Using source bucket: {source_bucket}")
    print(f"Using target bucket: {target_bucket}")
    print("Pattern configurations:")
    for i, (pattern, replacement, ending) in enumerate(pattern_configs):
        print(f"  {i+1}. '{pattern}...' -> '{replacement}' (ending with '{ending}')")
    print("  4. Wrapping bare URLs from https://scenes.s3.amazonaws.com/ with image pattern")
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # List all objects in the source bucket
    print(f"Listing objects in {source_bucket}...")
    response = s3_client.list_objects_v2(Bucket=source_bucket)
    
    # Filter for text files
    text_files = [item['Key'] for item in response.get('Contents', []) 
                 if item['Key'].endswith('.txt')]
    
    print(f"Found {len(text_files)} text files")
    
    # Process files sequentially
    successful = 0
    failed = 0
    
    for file_key in text_files:
        print(f"\nProcessing file: {file_key}")
        result = process_file(
            s3_client, 
            source_bucket, 
            target_bucket, 
            file_key,
            pattern_configs
        )
        
        if result:
            successful += 1
        else:
            failed += 1
    
    # Report the results
    print(f"\nProcessed {successful + failed} files. {successful} successful, {failed} failed.")

if __name__ == "__main__":
    main()