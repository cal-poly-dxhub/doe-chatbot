
import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rename_objects_with_spaces(bucket_name, dry_run=False):
    """
    Iterates through all objects in the specified S3 bucket and renames objects 
    containing spaces in their key name by replacing spaces with underscores.
    
    Args:
        bucket_name (str): Name of the S3 bucket to process
        dry_run (bool): If True, only show what would be done without making changes
    """
    s3_client = boto3.client('s3')
    
    # Use paginator to handle buckets with more than 1000 objects
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    
    renamed_count = 0
    
    for page in page_iterator:
        if 'Contents' not in page:
            logger.info(f"No objects found in bucket {bucket_name}")
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            
            # Check if the key contains spaces
            if ' ' in key:
                new_key = key.replace(' ', '_')
                
                logger.info(f"Found object with spaces: {key}")
                logger.info(f"Will rename to: {new_key}")
                
                if not dry_run:
                    # Copy the object to the new key
                    logger.info(f"Copying {key} to {new_key}...")
                    s3_client.copy_object(
                        CopySource={'Bucket': bucket_name, 'Key': key},
                        Bucket=bucket_name,
                        Key=new_key
                    )
                    
                    # Delete the old object
                    logger.info(f"Deleting original object {key}...")
                    s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=key
                    )
                    
                    logger.info(f"Successfully renamed {key} to {new_key}")
                else:
                    logger.info(f"DRY RUN: Would rename {key} to {new_key}")
                
                renamed_count += 1
    
    if renamed_count == 0:
        logger.info(f"No files with spaces found in bucket {bucket_name}")
    else:
        action_text = "Would rename" if dry_run else "Renamed"
        logger.info(f"{action_text} {renamed_count} objects in bucket {bucket_name}")

def main():
    bucketName = "francischatbotstack-inputassetsbucket8ff52ce-aqruaqp2srw3"
    dry_run = False
    logger.info(f"Starting to process bucket: {bucketName}")
    if dry_run:
        logger.info("Running in DRY RUN mode - no changes will be made")
    
    try:
        rename_objects_with_spaces(bucketName, dry_run)
        logger.info("Processing complete")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())
