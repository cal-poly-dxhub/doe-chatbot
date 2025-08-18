# ECS Embeddings Migration

## Overview
The embeddings processing has been migrated from AWS Lambda to Amazon ECS Fargate to provide:
- **3 concurrent task execution** as requested
- **No timeout limitations** (unlike Lambda's 15-minute limit)
- **Improved resource utilization** with dedicated compute resources
- **Better monitoring and observability** through ECS CloudWatch metrics

## Architecture Changes

### Before (Lambda)
- Docker-based Lambda function with 15-minute timeout
- Step Functions DistributedMap with max concurrency 5
- ARM64 architecture with 2048MB memory

### After (ECS)
- ECS Fargate cluster with dedicated task definitions
- Step Functions DistributedMap with max concurrency **3** as requested
- ARM64 Fargate tasks with **8 vCPU and 32GB memory** for heavy processing
- **No timeout restrictions** - tasks can run for 3+ hours or indefinitely

## Key Components

### 1. ECS Infrastructure
- **ECS Cluster**: `EmbeddingsCluster` with container insights enabled
- **Task Definition**: Fargate task with **8 vCPU and 32GB RAM** for heavy embeddings processing
- **Task Role**: All the same IAM permissions as the original Lambda
- **Security**: VPC-based networking in private subnets
- **Runtime**: Unlimited execution time (can run 3+ hours or more)

### 2. Container Setup
- **Base Image**: `python:3.11-slim` (ARM64)
- **Dockerfile**: `lib/backend/ingestion/embeddings/Dockerfile.ecs`
- **Entrypoint**: Python script that reads Step Functions input and processes embeddings
- **Dependencies**: Same requirements.txt as the original Lambda

### 3. Step Functions Integration
- **ECS Run Task**: Replaces Lambda invocation
- **Input Passing**: Uses environment variables to pass Step Functions payload
- **Integration Pattern**: `RUN_JOB` for synchronous execution
- **Networking**: Private subnets with egress for AWS service access

## Environmental Variables
All the same environment variables from the Lambda are preserved:
- `CACHE_TABLE_NAME`
- `EMBEDDINGS_SAGEMAKER_MODELS`
- `LINKS_TABLE_NAME`
- `MEDIA_BUCKET_NAME`
- `RDS_SECRET_ARN`
- `RDS_ENDPOINT`
- `CHUNK_SIZE_DOC_SPLIT`
- `OVERLAP_FOR_DOC_SPLIT`
- Plus Step Functions input via `INPUT_JSON`

## IAM Permissions
The ECS task role includes all the same permissions as the original Lambda:
- S3 read/write access to input, processed, and media buckets
- DynamoDB read/write access to cache and links tables
- RDS secret read access
- Textract document analysis permissions
- Bedrock model invocation (for embedding models)
- SageMaker endpoint invocation (for embedding models)

## Monitoring & Logging
- **CloudWatch Logs**: All task output goes to dedicated log group `/aws/ecs/embeddings`
- **Task Metrics**: Standard ECS metrics available in CloudWatch
- **Step Functions Integration**: Full execution tracking through Step Functions console

## Resource Specifications
- **CPU**: 8 vCPU (ARM64 architecture)
- **Memory**: 32 GB RAM
- **Runtime**: Unlimited (can run 3+ hours or indefinitely)
- **Platform**: AWS Fargate with dedicated compute allocation

## Concurrency Control
- **Maximum Concurrent Tasks**: 3 (as requested)
- **Automatic Scaling**: Tasks are launched on-demand by Step Functions
- **Resource Isolation**: Each task runs in its own high-performance Fargate instance

## Benefits of Migration
1. **Unlimited Runtime**: Tasks can run for 3+ hours or indefinitely (vs 15-minute Lambda limit)
2. **High Performance**: 8 vCPU and 32GB RAM for intensive embeddings processing
3. **Cost Efficiency**: Pay per second of task execution, no cold starts
4. **Better Resource Control**: Dedicated high-performance compute allocation
5. **Improved Observability**: Enhanced monitoring through ECS metrics
6. **Scalability**: Easy to adjust task resources or concurrency as needed

## Output Compatibility
The ECS implementation maintains **100% compatibility** with the original Lambda approach:
- **Same S3 Output Location**: Results written to `ingestion_output/{execution_name}/sf-results/`
- **Same JSON Format**: `{"FileURI": "s3://...", "EmbeddingsGenerated": 123}` 
- **Same File Naming**: Unique result files per processed document
- **Same Integration**: Works seamlessly with existing downstream processes

## Deployment
The migration is transparent to users:
- Same Step Functions state machine
- Same input/output format and location
- Same error handling and retry behavior
- Same CloudFormation stack deployment process
- **Zero breaking changes** to existing workflows

## Files Changed
- `lib/infra/ingestion/pipeline.ts` - Main infrastructure changes
- `lib/backend/ingestion/embeddings/Dockerfile.ecs` - New ECS Dockerfile
- `lib/backend/ingestion/embeddings/entrypoint.py` - ECS container entrypoint

## Rollback Plan
If needed, the Lambda-based approach can be restored by:
1. Reverting changes to `lib/infra/ingestion/pipeline.ts`
2. Removing the ECS-specific files
3. Redeploying the stack

This migration provides a more robust, scalable, and unlimited processing capability for embeddings generation. 