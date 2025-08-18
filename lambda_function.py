import json
import os
import psycopg2
import logging
from botocore.exceptions import ClientError
from rds_util import embed_query, store_embedded_query, get_plaintext_document
import shutil
import boto3
from urllib.parse import urlparse
import re
import random

# set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# api client for websocket api
api_client = boto3.client('apigatewaymanagementapi', 
endpoint_url="https://ff095qs246.execute-api.us-west-2.amazonaws.com/dev/")
bedrock_client = boto3.client("bedrock-runtime", region_name="us-west-2")
modelIDs = ['anthropic.claude-3-5-sonnet-20241022-v2:0',
            'anthropic.claude-3-5-sonnet-20240620-v1:0']


dynamo = boto3.resource('dynamodb')
table = dynamo.Table('message-history')

def getMessageSession(connectionID):
    logger.info(f"Looking up connectionID: {connectionID}")
    response = table.get_item(Key={'connectionID': f"{connectionID}"}, ConsistentRead=True)
    logger.info(f"response: {response}")
    return response.get('Item',None)

def saveMessageSession(connectionID, documents, chatHistory):
    logger.info(f"Saving session for {connectionID}")
    table.put_item(Item={'connectionID': f"{connectionID}",
     "documents": documents,
      "chatHistory": chatHistory})

def send_response(connection_id, message):
    """Sends a response back to the WebSocket client."""
    try:
        api_client.post_to_connection(
            Data=json.dumps(message).encode('utf-8'),
            ConnectionId=connection_id
        )
    except ClientError as e:
        logger.error(f"Error posting to WebSocket: {e}")
        raise
 
def get_flag(user_query, chat_data):
    """Sends the user query to the LLM and returns the relevance flag."""
    ch = "\n\n".join([f'{dialog["role"]}: {dialog["content"]}' for dialog in chat_data["chatHistory"][-6:]])
    ad = "\n\n".join(chat_data["documents"][-3:])
    
    prompt = f'''
    You are a help-desk chatbot designed to classify user inquiries about the [CLIENT] software. Users will be newly hired staff or non-[CLIENT] employees learning to navigate the system. You will be provided with official documentation. Your task is to determine whether the question can be answered with those provided documentation or if more information is needed.
    ---

    ### **Classification Rules:**
    1. **If the question is relevant to [CLIENT SOFTWARE] or [CLIENT] (e.g., student records, enrollment, waitlists, attendance, grading, [CLIENT] portals, scheduling, etc.):**
    - ✅ **If the provided documentation fully answers the question, classify as:** `"CONTQ"`
    - ❌ **If provided documentation is missing, incomplete, or unclear, classify as:** `"NEWQ"`

    2. **If the question is completely unrelated to [CLIENT SOFTWARE] or [CLIENT] (e.g., finance, personal tech issues, unrelated government services), classify as:** `"INVALIDQ"`

    ---

    ### **Step-by-Step Evaluation Process:**
    1. **Assess if the question is about [CLIENT SOFTWARE] or [CLIENT].**  
    - Does it involve an [CLIENT SOFTWARE] function (e.g., logging in, scheduling, student data, permissions)?  
    - If yes → Proceed to Step 2.  
    - If no → `"INVALIDQ"`  

    2. **Check documentation availability.**  
    - If the **documentation provides a complete, clear answer**, return `"CONTQ"`.  
    - If documentation is **missing, vague, or only partially relevant**, return `"NEWQ"`.  
    - If the only reference is in **chat history but NOT in documentation**, return `"NEWQ"`.  

    ---

    ### **Examples & Classification:**
    - **"How do I add a student to the waitlist?"**  
    - ✅ Docs have full steps → `"CONTQ"`  
    - ❌ Docs mention waitlists but lack steps → `"NEWQ"`  
    - ❌ No mention of waitlists in docs → `"NEWQ"`  

    - **"How do I reset my [CLIENT SOFTWARE] password?"**  
    - ✅ Docs contain password reset steps → `"CONTQ"`  
    - ❌ Docs mention password but lack reset details → `"NEWQ"`  

    - **"How do I invest in stocks?"** → `"INVALIDQ"`  

    ---

    ### **Input Data:**
    - **User's Question:** `{user_query}`
    - **Recent Chat History:**  
    `{ad}`
    - **Available Documentation:**  
    `{ch}`
    '''

    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0.5,
        "messages": [{"role": "user", "content": prompt}]
    }
    # invoke model w/ prompt 1
    try:
        modelIdRandomGetFlag=random.choice(modelIDs)
        print("Get flag model id ",modelIdRandomGetFlag)
        response = bedrock_client.invoke_model(modelId=modelIdRandomGetFlag, body=json.dumps(native_request))
        model_response = json.loads(response["body"].read())
        if "content" in model_response:
            return model_response["content"][0]["text"].strip()
        else:
            logger.error("No content in model response")
            return "ERROR"
    except (ClientError, Exception) as e:
        logger.error(f"Bedrock model invocation error: {e}")
        return "ERROR"


def offtopic_answer(user_query, chat_data):
    """Sends the user query to the LLM and returns the relevance flag."""
    ch = "\n\n".join([f'{dialog["role"]}: {dialog["content"]}' for dialog in chat_data["chatHistory"][-6:]])
    prompt = f"""
        If the user says "hi" or greets, say something like:  
        "Hi! How can I help?"

        If their question might relate to [CLIENT SOFTWARE] but is unclear, say something like:  
        "Can you rephrase that or make it more specfic so I can help better?" 

        If their question is clearly off-topic, say:  
        "That seems unrelated to [CLIENT SOFTWARE]. If I’m wrong, can you rephrase?" 

        Keep it short and natural—no unnecessary intros.  

        User's message: {user_query}
        Chat History so far: {ch}
    """
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0.5,
        "messages": [{"role": "user", "content":prompt}]
    }
    # invoke model w/ prompt 1
    try:
        # response = bedrock_client.invoke_model(modelId="anthropic.claude-3-5-haiku-20241022-v1:0", body=json.dumps(native_request))
        response = bedrock_client.invoke_model(modelId='anthropic.claude-3-sonnet-20240229-v1:0', body=json.dumps(native_request))
        model_response = json.loads(response["body"].read())
        
        if "content" in model_response:
            answer = model_response["content"][0]["text"].strip()
            chat_data["chatHistory"].append({
                    "role": "assistant",
                    "content":  answer})  
            return answer
        else:
            logger.error("No content in model response")
            return "ERROR"
    except (ClientError, Exception) as e:
        logger.error(f"Bedrock model invocation error: {e}")
        return "ERROR"




def answer_question(user_query, chat_data):
    ''' answer the user's question '''
    prompt = f'''
    You are a help-desk chatbot designed to assist new users with the [CLIENT] software. Most users will be newly hired staff undergoing training or non-[CLIENT] employees learning how to navigate the system. You will be provided with official documentation containing step-by-step instructions. Your responses must be based strictly on this documentation. Do not reference or imply the existence of documents—respond as if you are the direct source of information.

    ### Response Guidelines:

    #### **Step-by-Step Assistance**  
    - Always provide **one step in a single message**.  
    - Wait for user confirmation before sending the next step.  
    - Format responses with ONE SINGLE STEP AT A TIME, e.g.:  
    **User:** *How can I do X?*  
    **Assistant:** *Step 1: Do A.*  
    **User:** *I finished Step 1.*  
    **Assistant:** *Step 2: Do B.*  

    #### **Clarity & Precision**  
    - Use clear, direct, and actionable instructions.  
    - Avoid theoretical explanations or unnecessary context.  

    #### **Handling Images**  
    - If a step includes an image link, keep the link in place.  
    - Do **not** describe the image—just reference it.  
    - NEVER skip an image. Under no circumstances should you skip an image!
    - The user should not have to ask for images. You should be providing them along with the current step that you are describing.
    ✅ *Click on the three vertical dots next to the assessment you wish to schedule.* [$Image: https://ingested-image.s3.amazonaws.com/##############.png$]  
    ❌ *Click on the three vertical dots next to the assessment you wish to schedule (this will open a dropdown where you can choose options).*  

    ### **In the case of videos**
    - If the Documentation contains a video link at the top, in this format: [Video Link:https://split-videos-bucket.s3.amazonaws.com/############.mp4] THEN
        - It is absolutely CRUCIAL that you say the following disclaimer at the beginning of your message: 'The information I will now give you has been taken from a live video demo. If you would like to see this video demo at any time, please ask me for it.'
    - Then, throughout the conversation with the user, if the user ever asks for the video, provide them with the [Video Link:https://split-videos-bucket.s3.amazonaws.com/############.mp4], AS WELL AS the [$VIDEO_TIMESTAMP{12.625}] for the CURRENT STEP that the user is on with you!
    - Whenever you do provide the video link, ensure it is in the correct formatting with brackets: [Video Link:https://split-videos-bucket.s3.amazonaws.com/############.mp4]. THE VIDEO TIMESTAMP MUST BE IN THIS FORMAT [$VIDEO_TIMESTAMP{12.625}]!!!
    - Unless the user explicitly asks for it, do not provide them with the video link or the video timestamp.

    #### **Troubleshooting & Follow-Ups**  
    - Anticipate clarifications and guide users step by step.  
    - If an issue isn't covered in the documentation, suggest logical next actions:  
    - Checking with a supervisor  
    - Contacting IT support  
    - Reviewing training materials  
    - Never speculate. If no clear solution exists, say you don’t know and direct the user to [CLIENT] support.  
    '''
    print("RAG DOCUMENTS: ", chat_data["documents"])
    messages = [
        {"role": "user", "content": "System Instructions: " + prompt},
        {"role":"assistant","content": "I understand."},
        {"role": "user", "content": "Documentation: " + " ".join(chat_data["documents"])},
        {"role":"assistant","content": "I understand. Please ask me a question about the [CLIENT] software."}
    ]
    
    # Ensure messages alternate properly
    prev_role = "assistant"  # Last role in the initial messages array
    for msg in chat_data["chatHistory"]:
        current_role = msg["role"]
        # If we have consecutive messages with the same role
        if current_role == prev_role:
            # Insert empty message with opposite role
            opposite_role = "user" if current_role == "assistant" else "assistant"
            messages.append({"role": opposite_role, "content": "ok"})
        
        messages.append(msg)
        prev_role = current_role
    
    # Make sure the final message has the user role for the current query
    if prev_role == "user":
        messages.append({"role": "assistant", "content": "ok"})
    
    messages.append({"role": "user", "content": user_query})
    
    # Rest of your function remains the same
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4012,
        "temperature": 0.5,
        "messages": messages
    }

    try:
        modelIDRandom=random.choice(modelIDs)
        print("answer question modelID: ", modelIDRandom)
        response = bedrock_client.invoke_model(modelId=modelIDRandom, body=json.dumps(native_request))
        model_response = json.loads(response["body"].read())
        if "content" in model_response:
            answer = model_response["content"][0]["text"].strip()
            chat_data["chatHistory"].append({
                    "role": "assistant",
                    "content":  answer})  
            print("Answer question chat history: ")
            for msg in chat_data["chatHistory"]:
                print(msg)
            print("Answer question chat history completed.")
            return answer
        else:
            logger.error("No content in model response")
            return "ERROR"
    except (ClientError, Exception) as e:
        logger.error(f"Bedrock model invocation error: {e}")
        return "ERROR"

def get_presigned_url(s3_url):
    """Convert an S3 URL to a pre-signed URL."""
    s3client = boto3.client('s3')
    expiration = 3600  # URL expires in 1 hour
    logger.info(f"S3 URL: {s3_url}")

    # Clean the URL of any \$ characters first
    s3_url = s3_url.rstrip('\$').replace('\$', '')
    
    parsed_url = urlparse(s3_url)
    
    # Extract bucket name and object key
    bucket_name = parsed_url.netloc.split(".")[0]  # Assumes format: bucket.s3.amazonaws.com
    object_key = parsed_url.path.lstrip("/")  # Remove leading slash
    
    try:
        presigned_url = s3client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expiration,
        )
        logger.info(f"Presigned URL: {presigned_url}")
        return presigned_url
    except Exception as e:
        logger.info("Get presigned url error:",e)
        return None



def lambda_handler(event, context):
    try:
        # extract connection ID and user query from the event
        connection_id = event['requestContext']['connectionId']
        logger.info(f"Connection ID: {connection_id}")
        user_query = json.loads(event['body'])['message']
        logger.info(f"User Query: {user_query}")
        identifier = json.loads(event['body'])['id']
        logger.info(f"ID: {identifier}")

        # check if connection ID document is present
        # if present then don't need to RAG again
        chat_data = getMessageSession(identifier)
        logger.info(f"Chat Data: {chat_data}")
        if not chat_data:
            logger.info("NO CHAT")
            chat_data = {"connectionID": f"{identifier}", "documents": [], "chatHistory": []}
        # saving pre-rag chat history
        # chat_data['chatHistory'].append({"role": "user", "content": user_query})
        saveMessageSession(identifier, chat_data["documents"], chat_data['chatHistory'])   # note to self: make push instead of replace 
        logger.info(f"Saved chat history: {chat_data['chatHistory']}")
        # get flag, rerag each time
        # user_query = reformulate_question(user_query, chat_data)
        chat_data["chatHistory"].append({
                    "role": "user",
                    "content": user_query}) 
        flag = get_flag(user_query, chat_data)
        logger.info(f"Flag: {flag}")

        # embed + query the database if validQ
        if ("NEWQ" not in flag) and ("CONTQ" not in flag):
            # answer but ask the user to ask a question that is more specfic to [CLIENT SOFTWARE]
            resp = offtopic_answer(user_query, chat_data)
            logger.info(f"Off-Topic Answer: {resp}")
            send_response(connection_id, resp)
            chat_data["chatHistory"].append({
                    "role": "assistant",
                    "content": resp}) 
            saveMessageSession(identifier, chat_data["documents"], chat_data["chatHistory"])
            return {
                'statusCode': 200,
                'body': json.dumps('Message processed successfully')
            }
        if "NEWQ" in flag or (len(chat_data["chatHistory"]) == 0):
            embeded_query = embed_query(user_query)
            plaintext_document = get_plaintext_document(embeded_query)  # run embedding against rds
            logger.info(f"Plaintext Document: {plaintext_document}")
            logger.info(f"Type of plaintext_document: {type(plaintext_document)}")
            chat_data["documents"].append(plaintext_document)
            logger.info(f"Chat Data: {chat_data["documents"]}")
 
        # send response to client
        # switch to prompt2

        resp = answer_question(user_query, chat_data)
        logger.info(f"Answer Before Regex: {resp}")
        urls = []
        for line in resp.split('\n'):
            if r'[$Image:' in line:
                # Get everything between "[\$Image: " and "]"
                url = line.split(r'[$Image:')[1].split(']')[0].strip()
                # url = url.rstrip(r'$')
                urls.append(url)
        logger.info(f"URLS: {urls}")
        for url in urls:
            cleanUrl = url.replace("%20"," ")
            presigned_url = get_presigned_url(cleanUrl)
            if presigned_url:
                resp = resp.replace(fr"[$Image: {url}]", fr"[$Image: {presigned_url}]")
        logger.info(f"RespImageSubWithPresigned: {resp}")

        video_urls = []
        for line in resp.split('\n'):
            if r'[Video Link:' in line:
                url = line.split(r'[Video Link:')[1].split(']')[0].strip()
                video_urls.append(url)
        logger.info(f"Video URLs: {video_urls}")
        for video_url in video_urls:
            cleanUrl = video_url.replace("%20", " ")
            presigned_url = get_presigned_url(cleanUrl)
            if presigned_url:
                resp = resp.replace(f"[Video Link:{video_url}]", f"[Video Link:{presigned_url}]")
                resp = resp.replace(f"[Video Link: {video_url}]", f"[Video Link:{presigned_url}]")
        logger.info(f"RespVideoSubWithPresigned: {resp}")

        send_response(connection_id, resp)
        saveMessageSession(identifier, chat_data["documents"], chat_data["chatHistory"])
        
        return {
            'statusCode': 200,
            'body': json.dumps('Message processed successfully')
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing message')
        }

