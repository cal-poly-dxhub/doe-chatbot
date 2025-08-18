from docx import Document
import boto3
import zipfile
import re
import os
import time
import random
import zipfile
from bs4 import BeautifulSoup
import io
import string
import urllib.parse

s3 = boto3.client('s3')
BUCKET_NAME = os.getenv("MEDIA_BUCKET_NAME")

def extract_text_with_images_from_docx(docx_stream: io.BytesIO, linksTable) -> str:
    with zipfile.ZipFile(docx_stream, 'r') as zip_ref:
        temp_dir = "/tmp/temp_unzip"
        os.makedirs(temp_dir, exist_ok=True)
        zip_ref.extractall(temp_dir)


    document = Document(docx_stream)
    with open(f"{temp_dir}/word/document.xml") as xml:
        soup = BeautifulSoup(xml, "xml")

    extracted_text = []
    paragraphs = soup.find_all("w:p")
    for paragraph in paragraphs:
        for child in paragraph.children:
            if text := child.find_all("w:t"):
                for t in text:
                    extracted_text.append(t.get_text())

            if images := child.find_all("a:blip"):
                for i in images:
                    if id := i.get("r:embed"):
                        try:
                            blob = document.part.rels[id].target_part.blob
                            filename_only = os.path.basename(document.part.rels[id].target_ref)
                            uuid = blob_to_s3_link(blob, filename_only, linksTable)
                            extracted_text.append(f"\n(!?#Image:{uuid})\n")
                        except Exception as e:
                            print("Failed to upload image")

    return ' '.join(extracted_text)


def blob_to_s3_link(blob, filename, linksTable):
    temp_file = io.BytesIO(blob)
    s3_key = f"{int(time.time() * 10000)}_{random.randint(1, 100_000_000)}_{filename}"
    s3.upload_fileobj(temp_file, BUCKET_NAME, s3_key)

    s3_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{s3_key}"

    # Generate a unique 5-character UUID
    uuid_candidate = None
    max_attempts = 100
    for _ in range(max_attempts):
        candidate = generate_short_uuid()
        try:
            response = linksTable.get_item(Key={"uuid": candidate})
            if "Item" not in response:
                uuid_candidate = candidate
                break  # it's unique
        except Exception as e:
            print(f"Error checking for UUID existence: {e}")
            break

    if not uuid_candidate:
        raise RuntimeError("Failed to generate a unique UUID after several attempts")

    # Write to DynamoDB
    try:
        linksTable.put_item(
            Item={
                "uuid": uuid_candidate,
                "original_link": s3_url,
                "type": "image",
                "created_at": int(time.time())
            }
        )
    except Exception as e:
        print(f"Failed to write to links table: {e}")
        raise

    return uuid_candidate

def generate_short_uuid(length=5):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
