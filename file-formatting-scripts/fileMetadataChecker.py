import boto3
import csv
from botocore.exceptions import ClientError

# Set your parameters directly here
BUCKET_NAME = "plaintext-all"  # Replace with your bucket name
OUTPUT_FILE = "s3_metadata_report.csv"  # Output filename

# Initialize AWS client
s3_client = boto3.client('s3')

# List to store results
results = []

def get_object_metadata(bucket, key):
    try:
        # Get object metadata
        response = s3_client.head_object(Bucket=bucket, Key=key)
        
        # Extract useful metadata
        metadata = {
            'Key': key,
            'ContentType': response.get('ContentType', 'N/A'),
            'ContentLength': response.get('ContentLength', 'N/A'),
            'ETag': response.get('ETag', 'N/A').strip('"'),
            'LastModified': response.get('LastModified', 'N/A'),
        }
        
        # Check if this is a text file that would be accepted by the pipeline
        acceptable = metadata['ContentType'] in ["text/plain", "text/csv", "application/csv"]
        metadata['AcceptableForIngestion'] = acceptable
        
        print(f"Processed: {key} - Content Type: {metadata['ContentType']} - Acceptable: {acceptable}")
        return metadata
    
    except ClientError as e:
        print(f"Error processing {key}: {e}")
        return {
            'Key': key,
            'ContentType': 'ERROR',
            'ContentLength': 'ERROR',
            'ETag': 'ERROR',
            'LastModified': 'ERROR',
            'AcceptableForIngestion': False,
            'Error': str(e)
        }

def main():
    print(f"Analyzing bucket: {BUCKET_NAME}")
    
    # Get list of all objects in bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME)
    
    all_keys = []
    for page in page_iterator:
        if 'Contents' in page:
            all_keys.extend([item['Key'] for item in page['Contents']])
    
    print(f"Found {len(all_keys)} objects in the bucket")
    
    # Process objects sequentially
    for key in all_keys:
        results.append(get_object_metadata(BUCKET_NAME, key))
    
    # Count types
    content_type_counts = {}
    acceptable_count = 0
    unacceptable_count = 0
    
    for item in results:
        content_type = item['ContentType']
        content_type_counts[content_type] = content_type_counts.get(content_type, 0) + 1
        
        if item['AcceptableForIngestion']:
            acceptable_count += 1
        else:
            unacceptable_count += 1
    
    # Write results to CSV
    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        fieldnames = ['Key', 'ContentType', 'ContentLength', 'ETag', 'LastModified', 'AcceptableForIngestion']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for item in results:
            writer.writerow({k: v for k, v in item.items() if k in fieldnames})
    
    # Print summary
    print("\n--- Summary ---")
    print(f"Total files: {len(results)}")
    print(f"Files acceptable for ingestion: {acceptable_count}")
    print(f"Files NOT acceptable for ingestion: {unacceptable_count}")
    
    print("\nContent Type Distribution:")
    for content_type, count in content_type_counts.items():
        acceptable = "✓" if content_type in ["text/plain", "text/csv", "application/csv"] else "✗"
        print(f"  {acceptable} {content_type}: {count}")
    
    print(f"\nDetailed results written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

