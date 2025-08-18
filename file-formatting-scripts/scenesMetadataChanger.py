#!/usr/bin/env python3
import boto3
import mimetypes

def update_content_types(bucket_name):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Guess content type based on file extension
            guessed_type, _ = mimetypes.guess_type(key)
            if guessed_type is None:
                print(f"Skipping {key}: cannot guess content type")
                continue

            try:
                head = s3.head_object(Bucket=bucket_name, Key=key)
            except Exception as e:
                print(f"Error retrieving metadata for {key}: {e}")
                continue

            current_type = head.get('ContentType', '')
            if current_type != guessed_type:
                print(f"Updating {key}: {current_type} -> {guessed_type}")
                try:
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': key},
                        Key=key,
                        ContentType=guessed_type,
                        MetadataDirective='REPLACE'
                    )
                except Exception as e:
                    print(f"Error updating {key}: {e}")
            else:
                print(f"No update needed for {key}")

if __name__ == '__main__':
    # not used when running cdk
    bucket_name = 'francischatbotstack-fr-mediabucketbcbb02ba-fizajw8tw0bh'
    update_content_types(bucket_name)
