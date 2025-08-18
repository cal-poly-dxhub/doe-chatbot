# Use AWS Lambda’s Python 3.11 base image
FROM --platform=linux/arm64 public.ecr.aws/lambda/python:3.11

# Install system dependencies
RUN yum update -y && \
    yum install -y ffmpeg && \
    yum clean all

# Copy requirements and install dependencies
# This is done first to leverage Docker layer caching
COPY ingestion/embeddings/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -t /opt/python

# Copy the toolkit code into the python path so it can be imported
COPY layers/toolkit-layer/python/francis_toolkit /opt/python/francis_toolkit/

# Copy the rest of the application code from the embeddings directory
COPY ingestion/embeddings/ ./

# Tell Lambda which handler to invoke
CMD ["lambda.handler"]
