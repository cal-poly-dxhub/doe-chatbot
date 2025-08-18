import boto3
from boto3.dynamodb.conditions import Attr

# Set up
dynamodb = boto3.resource('dynamodb', "us-west-2")
table = dynamodb.Table('LinksTable') 

# Step 1: Scan for all items with type == "video"
def get_video_items():
    items = []
    last_evaluated_key = None

    while True:
        if last_evaluated_key:
            response = table.scan(
                FilterExpression=Attr('type').eq('video-image'),
                ExclusiveStartKey=last_evaluated_key
            )
        else:
            response = table.scan(FilterExpression=Attr('type').eq('video-image'))

        items.extend(response['Items'])
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    return items

# Step 2: Delete items by uuid
def delete_items(items):
    for item in items:
        uuid = item['uuid']
        try:
            table.delete_item(Key={'uuid': uuid})
            print(f"Deleted: {uuid}")
        except Exception as e:
            print(f"Failed to delete {uuid}: {e}")

if __name__ == "__main__":
    videos = get_video_items()
    print(f"Found {len(videos)} items with type='video'")
    delete_items(videos)
