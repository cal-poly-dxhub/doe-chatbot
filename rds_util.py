import boto3
import json
import logging
import os
import psycopg2
from psycopg2 import extras

# set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CONFIGURATION =======
# bedrock - embeddings
bedrock = boto3.client(service_name='bedrock-runtime', region_name='us-west-2')
model_id = 'amazon.titan-embed-text-v2:0'

# get password(s) from secret manager
secrets_manager = boto3.client('secretsmanager')
logger.info(f"TEST: {os.getenv('RDS_SECRET_ARN')}")
region_name = "us-west-2"
client = boto3.client(
    service_name='secretsmanager',
    region_name=region_name
)
try:
    get_secret_value_response = client.get_secret_value(
        SecretId=os.getenv('RDS_SECRET_ARN')
    )
except ClientError as e:
    raise e

DB_CRED = json.loads(get_secret_value_response['SecretString'])

# rds
DB_USER = os.getenv('DB_USER')
RDS_HOST = os.getenv('RDS_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT')
logger.info(f"RDS_HOST: {RDS_HOST}, DB_NAME: {DB_NAME}, DB_PORT: {DB_PORT}")
# ============================

def get_plaintext_document(embedded_query):
    """Finds the most similar document to user query"""
    logger.info(f"DB_PASSWORD: {DB_CRED}")
    with psycopg2.connect(
              host=RDS_HOST,
              dbname=DB_NAME,
              user=DB_CRED["username"],
              password=DB_CRED["password"],
              port=DB_PORT
        ) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Find the document with the smallest distance (most similar)
                cur.execute("""
                            SELECT metadata->>'plain_text' AS plain_text, embedding <=> %s::vector AS distance
                            FROM txts
                            ORDER BY distance ASC
                            LIMIT 1""", (embedded_query,))
                result = cur.fetchone()
                
                if result:
                    logger.info("Successfully retrieved document from RDS.")
                    return result["plain_text"]
                else:
                    logger.info("No relevant document found.")
                    return "No relevant document found."

def store_embedded_query(query, embedding):
    """Stores embedded query & query string into RDS"""
    try:
        with psycopg2.connect(
              host=RDS_HOST,
              dbname=DB_NAME,
              user=DB_USER,
              password=DB_PASSWORD,
              port=DB_PORT
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO embedded_queries (query_text, embedding)
                    VALUES (%s, %s)
                """, (query, json.dumps(embedding)))
                conn.commit()
                logger.info("Successfully stored embedding in RDS.")
    except Exception as e:
        logger.error(f"Failed to store embedding in RDS: {e}")

        
def embed_query(query):
    """Embeds the valid query"""
    embedding_request = {
        "inputText": query
    }
    accept = 'application/json'
    content_type = 'application/json'

    try:
        response = bedrock.invoke_model(
            body=json.dumps(embedding_request),
            modelId=model_id,
            accept=accept,
            contentType=content_type
        )
        logger.info(response)

        model_response = json.loads(response["body"].read())
        embedding = model_response["embedding"]
        return embedding
  
    except Exception as e:
        print(f"Error during model invocation: {e}")
        return None
