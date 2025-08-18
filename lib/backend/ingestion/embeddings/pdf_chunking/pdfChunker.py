import os
import re
import json
import boto3
from botocore.config import Config
from typing import List, Dict, Tuple, Optional, Union, Any
from textractor.data.text_linearization_config import TextLinearizationConfig
import requests
from urllib.parse import urlparse
import asyncio
from .aws_utils import *
from .table_tools import *

config = Config(
    read_timeout=600,
    retries=dict(
        max_attempts=5
    )
)

REGION_NAME = os.getenv('REGION_NAME')
MEDIA_BUCKET_NAME = os.getenv('MEDIA_BUCKET_NAME')

s3 = boto3.client('s3', region_name=REGION_NAME)

bedrock_runtime = boto3.client(service_name='bedrock-runtime', region_name=REGION_NAME, config=config)


def strip_newline(cell: Any) -> str:
    """Remove newline characters from a cell value."""
    return str(cell).strip()

def sub_header_content_splitter(string: str) -> List[str]:
    """Split content by XML tags and return relevant segments."""
    pattern = re.compile(r'<<[^>]+>>')
    segments = re.split(pattern, string)
    result = []
    for segment in segments:
        if segment.strip():
            if "<header>" not in segment and "<list>" not in segment and "<table>" not in segment:
                segment = [x.strip() for x in segment.split('\n') if x.strip()]
                result.extend(segment)
            else:
                result.append(segment)
    return result

def split_list_items_(items: str) -> List[str]:
    """Split a string into a list of items, handling nested lists."""
    parts = re.split("(<<list>><list>|</list><</list>>)", items)
    output = []

    inside_list = False
    list_item = ""

    for p in parts:
        if p == "<<list>><list>":
            inside_list = True
            list_item = p
        elif p == "</list><</list>>":
            inside_list = False
            list_item += p
            output.append(list_item)
            list_item = ""
        elif inside_list:
            list_item += p.strip()
        else:
            output.extend(p.split('\n'))
    return output

def process_document(document, local_pdf_path: str) -> Tuple[Dict, Dict]:
    """Process a document from textract, extract different items."""

    config = TextLinearizationConfig(
        hide_figure_layout=True,
        hide_table_layout=True, 
        title_prefix="<titles><<title>><title>",
        title_suffix="</title><</title>>",
        hide_header_layout=True,
        section_header_prefix="<headers><<header>><header>",
        section_header_suffix="</header><</header>>",
        table_prefix="<tables><table>",
        table_suffix="</table>",
        # figure_layout_prefix="<figure><figure>",
        # figure_layout_suffix="</figure>",
        list_layout_prefix="<<list>><list>",
        list_layout_suffix="</list><</list>>",
        hide_footer_layout=True,
        hide_page_num_layout=True,
    )

    csv_seperator = "|"  # "\t"
    document_holder = {}
    table_page = {}
    count = 0
    table_strings = []
    # Whether to handle merged cells by duplicating merged value across corresponding individual cells
    unmerge_span_cells = True
    # Loop through each page in the document
    for ids, page in enumerate(document.pages):
        table_count = len([word for word in page.get_text(config=config).split() if
                           "<tables><table>" in word])
        # assert table_count == len(page.tables)
        content = page.get_text(config=config).split("<tables>")
        document_holder[ids] = []
        for idx, item in enumerate(content):
            if "<table>" in item:
                table = document.tables[count]

                bounding_box = table.bbox

                table_pg_number = table.page

                table_base64 = get_table_base64_from_pdf(local_pdf_path, table_pg_number, bounding_box)

                if ids in table_page:
                    table_page[ids].append(table_base64)
                else:
                    table_page[ids] = [table_base64]

                
                # Extract table data and remaining content
                pattern = re.compile(r'<table>(.*?)(</table>)', re.DOTALL)
                data = item
                table_match = re.search(pattern, data)
                table_data = table_match.group(1) if table_match else ''
                remaining_content = data[table_match.end():] if table_match else data

                content[idx] = f"<<table>><table>{table_base64}</table><</table>>"  ## attach xml tags to differentiate table from other text
                count += 1

                if "<<list>>" in remaining_content:
                    output = split_list_items_(remaining_content)
                    output = [x.strip() for x in output if x.strip()]
                    document_holder[ids].extend([content[idx]] + output)
                else:
                    document_holder[ids].extend([content[idx]] + [x.strip() for x in remaining_content.split('\n') if
                                                                  x.strip()]) # split other text by new line to be independent items in the python list.
            else:
                if "<<list>>" in item and "<table>" not in item:
                    output = split_list_items_(item)
                    output = [x.strip() for x in output if x.strip()]
                    document_holder[ids].extend(output)
                else:
                    document_holder[ids].extend([x.strip() for x in item.split("\n") if x.strip()])

    page_mapping = {}
    current_page = 1
    
    for page in document.pages:
        page_content = page.get_text(config=config)
        page_mapping[current_page] = page_content
        current_page += 1

    # Flatten the nested list document_holder into a single list and Join the flattened list by "\n"
    flattened_list = [item for sublist in document_holder.values() for item in sublist]
    result = "\n".join(flattened_list)
    header_split = result.split("<titles>")

    return header_split, page_mapping

def chunk_document(header_split, file, BUCKET, page_mapping):
    """Document chunking"""
    csv_seperator = "|"
    max_words = 800
    chunks = {}
    table_header_dict = {}
    chunk_header_mapping = {}
    list_header_dict = {}

    # Function to find the page number for a given content
    def find_page_number(content):
        for page_num, page_content in page_mapping.items():
            if content in page_content:
                return page_num
        return None

    # iterate through each title section
    for title_ids, items in enumerate(header_split):
        title_chunks = []
        current_chunk = {"content": [], "metadata": {}}
        num_words = 0
        table_header_dict[title_ids] = {}
        chunk_header_mapping[title_ids] = {}
        list_header_dict[title_ids] = {}
        chunk_counter = 0
        last_known_page = 1

        doc_id = os.path.basename(file)

        for item_ids, item in enumerate(items.split('<headers>')):  # headers
            lines = sub_header_content_splitter(item)
            SECTION_HEADER = None
            TITLES = None
            num_words = 0

            for ids_line, line in enumerate(lines):  # header lines
                if not line.strip():
                    continue

                # Find the page number for this line
                page_number = find_page_number(line)
                if page_number:
                    last_known_page = page_number
                current_chunk["metadata"]["page"] = last_known_page

                # Handle titles
                if "<title>" in line:
                    TITLES = re.findall(r'<title>(.*?)</title>', line)[0].strip()
                    line = TITLES
                    current_chunk["metadata"]["title"] = TITLES
                    if re.sub(r'<[^>]+>', '', "".join(lines)).strip() == TITLES:
                        chunk_header_mapping[title_ids][chunk_counter] = lines
                        chunk_counter += 1

                # Handle section headers
                if "<header>" in line:
                    SECTION_HEADER = re.findall(r'<header>(.*?)</header>', line)[0].strip()
                    line = SECTION_HEADER
                    current_chunk["metadata"]["section_header"] = SECTION_HEADER
                    first_header_portion = True

                # Count words if we were to add this line
                next_num_words = num_words + len(re.findall(r'\w+', line))

                # For tables and lists, keep page metadata
                if "<table>" in line or "<list>" in line:
                    current_chunk["metadata"]["page"] = last_known_page

                # If it's plain text, check for overflow
                if "<table>" not in line and "<list>" not in line:
                    if next_num_words > max_words:
                        # 1) Harvest last 50 words of the current chunk
                        flat = " ".join(current_chunk["content"])
                        words = re.findall(r'\w+', flat)
                        overlap = words[-50:] if len(words) >= 50 else words
                        overlap_text = " ".join(overlap)

                        # 2) Append overlap onto the old chunk (~850 words) and flush it
                        current_chunk["content"].append(overlap_text)
                        title_chunks.append(current_chunk)
                        chunk_header_mapping[title_ids][chunk_counter] = lines

                        # 3) Seed the new chunk with the same overlap
                        current_chunk = {
                            "content": [overlap_text],
                            "metadata": {}
                        }
                        if SECTION_HEADER:
                            current_chunk["metadata"]["section_header"] = SECTION_HEADER
                        if TITLES:
                            current_chunk["metadata"]["title"] = TITLES

                        # 4) Reset word count to the overlap size
                        num_words = len(overlap)
                        chunk_counter += 1

                    # Add the current line to whichever chunk we're in
                    current_chunk["content"].append(line)
                    num_words += len(re.findall(r'\w+', line))

                # Handle tables
                if "<table>" in line:
                    # Get table header which is usually line before table in document
                    line_index = lines.index(line)
                    if line_index != 0 and "<table>" not in lines[line_index - 1] and "<list>" not in lines[line_index - 1]:
                        header = lines[line_index - 1].replace("<header>", "").replace("</header>", "")
                    else:
                        header = ""

                    # Extract the base64 data
                    table_base64 = re.search(r'<table>(.*?)</table>', line).group(1)
                    # Add the table as a whole to the current chunk
                    current_chunk["content"].append(f"<table>{header}<base64>{table_base64}</base64></table>")
                    # Reset num_words to 0 as we're not counting words in the table
                    num_words = 0

                # Handle lists
                if "<list>" in line:
                    # Get list header which is usually line before list in document
                    line_index = lines.index(line)
                    if line_index != 0 and "<table>" not in lines[line_index - 1] and "<list>" not in lines[line_index - 1]:
                        header = lines[line_index - 1].replace("<header>", "").replace("</header>", "")
                    else:
                        header = ""
                    list_pattern = re.compile(r'<list>(.*?)(?:</list>|$)', re.DOTALL)
                    list_match = re.search(list_pattern, line)
                    list_ = list_match.group(1)
                    list_lines = list_.split("\n")

                    curr_chunk = []
                    words = len(re.findall(r'\w+', str(current_chunk)))
                    for lyst_item in list_lines:
                        curr_chunk.append(lyst_item)
                        words += len(re.findall(r'\w+', lyst_item))
                        if words >= max_words:
                            if [x for x in list_header_dict[title_ids] if chunk_counter == x]:
                                list_header_dict[title_ids][chunk_counter].extend([header] + [list_])
                            else:
                                list_header_dict[title_ids][chunk_counter] = [header] + [list_]
                            words = 0
                            list_chunk = "\n".join(curr_chunk)
                            if header:
                                if current_chunk['content'] and current_chunk["content"][-1].strip().lower() == header.strip().lower():
                                    current_chunk["content"].pop()
                                if SECTION_HEADER and SECTION_HEADER.lower().strip() != header.lower().strip():
                                    if first_header_portion:
                                        first_header_portion = False
                                    else:
                                        current_chunk["content"].insert(0, SECTION_HEADER.strip())
                                current_chunk["content"].extend([header.strip() + ':' if not header.strip().endswith(':') else header.strip()] + [list_chunk])
                                title_chunks.append(current_chunk)
                            else:
                                if SECTION_HEADER:
                                    if first_header_portion:
                                        first_header_portion = False
                                    else:
                                        current_chunk["content"].insert(0, SECTION_HEADER.strip())
                                current_chunk["content"].extend([list_chunk])
                                title_chunks.append(current_chunk)
                            chunk_header_mapping[title_ids][chunk_counter] = lines
                            chunk_counter += 1
                            num_words = 0
                            current_chunk = {"content": [], "metadata": {}}
                            curr_chunk = []
                    if curr_chunk and lines.index(line) == len(lines) - 1:
                        list_chunk = "\n".join(curr_chunk)
                        if [x for x in list_header_dict[title_ids] if chunk_counter == x]:
                            list_header_dict[title_ids][chunk_counter].extend([header] + [list_])
                        else:
                            list_header_dict[title_ids][chunk_counter] = [header] + [list_]
                        if header:
                            if current_chunk["content"] and current_chunk["content"][-1].strip().lower() == header.strip().lower():
                                current_chunk["content"].pop()
                            if SECTION_HEADER and SECTION_HEADER.lower().strip() != header.lower().strip():
                                if first_header_portion:
                                    first_header_portion = False
                                else:
                                    current_chunk["content"].insert(0, SECTION_HEADER.strip())
                            current_chunk["content"].extend([header.strip() + ':' if not header.strip().endswith(':') else header.strip()] + [list_chunk])
                            title_chunks.append(current_chunk)
                        else:
                            if SECTION_HEADER:
                                if first_header_portion:
                                    first_header_portion = False
                                else:
                                    current_chunk["content"].insert(0, SECTION_HEADER.strip())
                            current_chunk["content"].extend([list_chunk])
                            title_chunks.append(current_chunk)
                        chunk_header_mapping[title_ids][chunk_counter] = lines
                        chunk_counter += 1
                        num_words = 0
                        current_chunk = {"content": [], "metadata": {}}
                    elif curr_chunk and lines.index(line) != len(lines) - 1:
                        list_chunk = "\n".join(curr_chunk)
                        if [x for x in list_header_dict[title_ids] if chunk_counter == x]:
                            list_header_dict[title_ids][chunk_counter].extend([header] + [list_])
                        else:
                            list_header_dict[title_ids][chunk_counter] = [header] + [list_]
                        if header:
                            if current_chunk["content"] and current_chunk["content"][-1].strip().lower() == header.strip().lower():
                                current_chunk["content"].pop()
                            current_chunk["content"].extend([header.strip() + ':' if not header.strip().endswith(':') else header.strip()] + [list_chunk])
                        else:
                            current_chunk["content"].extend([list_chunk])
                        num_words = words

            # After finishing all lines under this header, flush any remaining chunk:
            if current_chunk["content"] and "".join(current_chunk["content"]).strip() not in {SECTION_HEADER, TITLES}:
                title_chunks.append(current_chunk)
                chunk_header_mapping[title_ids][chunk_counter] = lines
                current_chunk = {"content": [], "metadata": {}}
                chunk_counter += 1

        if current_chunk["content"]:
            title_chunks.append(current_chunk)
            chunk_header_mapping[title_ids][chunk_counter] = lines
        chunks[title_ids] = title_chunks

    # List of title header sections document was split into
    for x in chunk_header_mapping:
        if chunk_header_mapping[x]:
            try:
                title_pattern = re.compile(r'<title>(.*?)(?:</title>|$)', re.DOTALL)
                title_match = re.search(title_pattern, chunk_header_mapping[x][0][0])
                title_ = title_match.group(1) if title_match else ""
            except:
                continue

    os.makedirs("/tmp/json", exist_ok=True)
    with open(f"/tmp/json/{doc_id}.json", "w") as f:
        json.dump(chunk_header_mapping, f, indent=4)
    # s3.upload_file(f"./test/tmp/{doc_id}.json", BUCKET, f"chunked_jsons/{doc_id}.json")
    # os.remove(f"/tmp/{doc_id}.json")

    doc = {
        'chunks': chunks,
        'chunk_header_mapping': chunk_header_mapping,
        'table_header_dict': table_header_dict,
        'list_header_dict': list_header_dict,
        'doc_id': doc_id
    }

    return doc



def parse_s3_uri(s3_uri):
    # Ensure the URI starts with "s3://"
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")
    
    # Remove the "s3://" prefix
    s3_path = s3_uri[5:]
    
    # Split the path into bucket and key
    bucket_name, *key_parts = s3_path.split("/", 1)
    file_key = key_parts[0] if key_parts else ""
    
    return bucket_name, file_key


def extract_clean_plaintext(doc_chunks, min_total_words=100) -> List[str]:
    """
    Takes structured document chunks, cleans them, applies quality filters,
    and returns a list of clean text strings.
    """
    all_cleaned_content = []

    junk_phrases = {
        "top of this section",
        "section header",
        "footer",
        "page x",
        "click here",
        "back to top"
    }

    def clean_line(line):
        if not isinstance(line, str):
            return ""
        return re.sub(r'<[^>]+>', '', line).strip()

    def is_junk_chunk(lines):
        cleaned = [clean_line(line).lower() for line in lines]
        return all(line in junk_phrases or not line for line in cleaned)

    def is_sentence(line):
        return line.endswith(('.', '?', '!')) and len(line.split()) >= 5

    def is_gibberish(line):
        words = line.split()
        real_words = [w for w in words if re.search(r'[aeiouAEIOU]', w) and len(w) > 2]
        return len(real_words) < max(3, len(words) * 0.4)

    for chunk_num, chunk_group in doc_chunks.items():
        valid_lines = []

        for chunk in chunk_group:
            if not isinstance(chunk, dict) or 'content' not in chunk:
                continue

            chunk_content = chunk['content']
            if not isinstance(chunk_content, list):
                continue

            if is_junk_chunk(chunk_content):
                continue

            cleaned_lines = [clean_line(line) for line in chunk_content if clean_line(line)]
            cleaned_lines = [line for line in cleaned_lines if not is_gibberish(line)]

            if not cleaned_lines:
                continue

            total_words = sum(len(line.split()) for line in cleaned_lines)
            sentence_count = sum(1 for line in cleaned_lines if is_sentence(line))
            avg_sentence_length = total_words / max(sentence_count, 1)

            # 🔒 Hardcore filters:
            if total_words < min_total_words:
                continue
            if sentence_count < 2:
                continue
            if avg_sentence_length < 8:
                continue

            valid_lines.extend(cleaned_lines)

        if valid_lines:
            content = "\n\n".join(valid_lines).strip()
            all_cleaned_content.append(content)

    return all_cleaned_content

def extract_raw_text_from_document(document) -> str:
    """
    Extract raw text from a Textract document without any filtering or chunking.
    This is used as a fallback when the filtered approach returns no chunks.
    
    Args:
        document: Textract document object
        
    Returns:
        str: Raw text content from the document
    """
    # Simple configuration for raw text extraction
    simple_config = TextLinearizationConfig(
        hide_figure_layout=True,
        hide_table_layout=False,  # Keep tables as text
        hide_header_layout=True,
        hide_footer_layout=True,
        hide_page_num_layout=True,
    )
    
    all_text = []
    for page in document.pages:
        page_text = page.get_text(config=simple_config)
        if page_text.strip():
            all_text.append(page_text.strip())
    
    # Join all pages with double newlines
    raw_text = "\n\n".join(all_text)
    
    # Basic cleanup - remove excessive whitespace and XML tags
    raw_text = re.sub(r'<[^>]+>', '', raw_text)  # Remove XML tags
    raw_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', raw_text)  # Collapse multiple newlines
    raw_text = re.sub(r'[ \t]+', ' ', raw_text)  # Normalize spaces
    
    return raw_text.strip()

def extract_raw_text_from_pdf_s3(bucket_name: str, s3_file_path: str) -> str:
    """
    Extract raw text from a PDF in S3 as a fallback when filtered chunking fails.
    
    Args:
        bucket_name (str): The S3 bucket name.
        s3_file_path (str): The object key for the PDF file.
        
    Returns:
        str: Raw text content from the PDF
    """
    s3_uri = f"s3://{bucket_name}/{s3_file_path}"
    print(f"Extracting raw text from {os.path.basename(s3_file_path)}")

    if not MEDIA_BUCKET_NAME:
        raise ValueError("MEDIA_BUCKET_NAME environment variable is not set.")

    textract_output_path = None
    try:
        document, local_pdf_path, textract_output_path = extract_textract_data(s3, s3_uri, bucket_name, MEDIA_BUCKET_NAME)
        raw_text = extract_raw_text_from_document(document)
        
        print(f"Extracted raw text from {os.path.basename(s3_file_path)} successfully.")
        return raw_text
    finally:
        # Clean up Textract output from the media bucket
        if textract_output_path:
            media_bucket, prefix = parse_s3_uri(textract_output_path)
            delete_s3_prefix(s3, media_bucket, prefix)

def process_pdf_from_s3(bucket_name: str, s3_file_path: str, document_url: str = "n/a") -> list:
    """
    Processes a PDF from S3 and returns a list of cleaned text chunks.

    Args:
        bucket_name (str): The S3 bucket name.
        s3_file_path (str): The object key for the PDF file.
        document_url (str, optional): The source URL of the document. Defaults to "n/a".

    Returns:
        list: A list of cleaned text chunks.
    """
    s3_uri = f"s3://{bucket_name}/{s3_file_path}"
    print(f"Processing {os.path.basename(s3_file_path)}")

    if not MEDIA_BUCKET_NAME:
        raise ValueError("MEDIA_BUCKET_NAME environment variable is not set.")

    textract_output_path = None
    try:
        document, local_pdf_path, textract_output_path = extract_textract_data(s3, s3_uri, bucket_name, MEDIA_BUCKET_NAME)

        header_split, page_mapping = process_document(document, local_pdf_path)

        doc_chunks = chunk_document(header_split, s3_file_path, bucket_name, page_mapping)
        
        cleaned_text_chunks = extract_clean_plaintext(doc_chunks['chunks'])

        print(f"Processed {os.path.basename(s3_file_path)} successfully.")
        return cleaned_text_chunks
    finally:
        # Clean up Textract output from the media bucket
        if textract_output_path:
            media_bucket, prefix = parse_s3_uri(textract_output_path)
            delete_s3_prefix(s3, media_bucket, prefix)

