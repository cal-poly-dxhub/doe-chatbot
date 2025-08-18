import boto3

# Initialize S3 client
s3_client = boto3.client('s3')

# Set your bucket name
BUCKET_NAME = "video-final-plaintext-without-irrelevant"

# Function to update content type
def update_content_type(bucket, key):
    try:
        # Copy the object to itself with the new content type
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=key,
            MetadataDirective='REPLACE',
            ContentType='text/plain'  # Set the correct content type
        )
        print(f"Updated content type for {key} to text/plain")
        return True
    except Exception as e:
        print(f"Error updating {key}: {e}")
        return False

# Get all text files in the bucket
paginator = s3_client.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=BUCKET_NAME)

updated_count = 0
for page in page_iterator:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('.txt'):
                if update_content_type(BUCKET_NAME, key):
                    updated_count += 1

print(f"\nSuccessfully updated content type for {updated_count} files")
