/*
Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as stepfn from 'aws-cdk-lib/aws-stepfunctions';
import * as stepfn_task from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as constants from '../common/constants';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import { BaseInfra } from '../base-infra';
import { Construct } from 'constructs';
import { NagSuppressions } from 'cdk-nag';
import { DefaultCorpusConfig } from '../common/types';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';

export interface IngestionPipelineProps {
    readonly baseInfra: BaseInfra;
    readonly rdsSecret: secretsmanager.ISecret;
    readonly rdsEndpoint: string;
    readonly inputAssetsBucket: s3.IBucket;
    readonly mediaBucket: s3.IBucket;
    readonly linksTable: ddb.ITable;
}

export class IngestionPipeline extends Construct {
    public readonly ingestionStateMachine: stepfn.StateMachine;

    public readonly embeddingsCluster: ecs.ICluster;
    public readonly embeddingsTaskDefinition: ecs.FargateTaskDefinition;
    public readonly inputValidationFunction: lambda.IFunction;

    public constructor(scope: Construct, id: string, props: IngestionPipelineProps) {
        super(scope, id);

        const startState = {
            /* eslint-disable @typescript-eslint/naming-convention */
            ['StateMachine.$' as 'StateMachine']: '$$.StateMachine' as unknown,
            ['Execution.$' as 'Execution']: '$$.Execution' as unknown,
            /* eslint-enable @typescript-eslint/naming-convention */
        };

        const cacheTable = new ddb.Table(this, 'CacheTable', {
            partitionKey: {
                name: 'PK',
                type: ddb.AttributeType.STRING,
            },
            sortKey: {
                name: 'SK',
                type: ddb.AttributeType.STRING,
            },
            billingMode: ddb.BillingMode.PAY_PER_REQUEST,
            encryption: ddb.TableEncryption.AWS_MANAGED,
            removalPolicy: props.baseInfra.removalPolicy,
            pointInTimeRecovery: true,
        });

        cacheTable.addGlobalSecondaryIndex({
            indexName: 'GSI1',
            partitionKey: {
                name: 'UpdatedStatus',
                type: ddb.AttributeType.STRING,
            },
            sortKey: {
                name: 'FileURI',
                type: ddb.AttributeType.STRING,
            },
        });

        const ingestionLambdaCommonProps = {
            ...constants.LAMBDA_COMMON_PROPERTIES,
            vpc: props.baseInfra.vpc,
            runtime: constants.LAMBDA_PYTHON_RUNTIME,
            memorySize: constants.INGESTION_LAMBDA_MEMORY_SIZE,
            handler: 'lambda.handler',
            timeout: cdk.Duration.minutes(15),
            layers: [
                props.baseInfra.powerToolsLayer,
                props.baseInfra.langchainLayer,
                props.baseInfra.toolkitLayer,
            ],
        };

        const cacheUpdateLambda = new lambda.Function(this, 'cacheUpdateFunction', {
            ...ingestionLambdaCommonProps,
            code: lambda.Code.fromAsset(
                path.join(constants.BACKEND_DIR, 'ingestion', 'cache_update')
            ),
            environment: {
                ...constants.LAMBDA_COMMON_ENVIRONMENT,
                /* eslint-disable @typescript-eslint/naming-convention */
                CACHE_TABLE_NAME: cacheTable.tableName,
                POWERTOOLS_SERVICE_NAME: 'ingestion-input-validation',
                /* eslint-enable @typescript-eslint/naming-convention */
            },
        });
        cacheTable.grantReadWriteData(cacheUpdateLambda);

        props.inputAssetsBucket.grantReadWrite(cacheUpdateLambda);
        props.inputAssetsBucket.addEventNotification(
            s3.EventType.OBJECT_CREATED,
            new s3n.LambdaDestination(cacheUpdateLambda)
        );
        props.inputAssetsBucket.addEventNotification(
            s3.EventType.OBJECT_REMOVED,
            new s3n.LambdaDestination(cacheUpdateLambda)
        );
        cacheTable.grantWriteData(cacheUpdateLambda);

        // Bucket containing the artifacts of ingestion pipeline
        const processedAssetsBucket = new s3.Bucket(this, 'processedAssetsBucket', {
            ...constants.BUCKET_COMMON_PROPERTIES,
            serverAccessLogsBucket: props.baseInfra.serverAccessLogsBucket,
        });

        const ingestionLambdaCommonEnvironment = {
            /* eslint-disable @typescript-eslint/naming-convention */
            METRICS_NAMESPACE: constants.METRICS_NAMESPACE,
            CACHE_TABLE_NAME: cacheTable.tableName,
            EMBEDDINGS_SAGEMAKER_MODELS: JSON.stringify(
                props.baseInfra.systemConfig.ragConfig.embeddingsModels
            ),
            PROCESSED_BUCKET_NAME: processedAssetsBucket.bucketName,
            /* eslint-enable @typescript-eslint/naming-convention */
        };

        // Lambda function used to validate inputs in the step function
        const inputValidationFunction = new lambda.Function(
            this,
            'inputValidationFunction',
            {
                ...ingestionLambdaCommonProps,
                code: lambda.Code.fromAsset(
                    path.join(constants.BACKEND_DIR, 'ingestion', 'input_validation')
                ),
                environment: {
                    ...constants.LAMBDA_COMMON_ENVIRONMENT,
                    ...ingestionLambdaCommonEnvironment,

                    /* eslint-disable @typescript-eslint/naming-convention */
                    POWERTOOLS_SERVICE_NAME: 'ingestion-input-validation',
                    /* eslint-enable @typescript-eslint/naming-convention */
                },
            }
        );
        props.inputAssetsBucket.grantRead(inputValidationFunction);
        processedAssetsBucket.grantWrite(inputValidationFunction);
        cacheTable.grantReadData(inputValidationFunction);

        const corpusConfig = props.baseInfra.systemConfig.ragConfig.corpusConfig as
            | DefaultCorpusConfig
            | undefined;

        // Create ECS Cluster for embeddings processing
        const embeddingsCluster = new ecs.Cluster(this, 'EmbeddingsCluster', {
            vpc: props.baseInfra.vpc,
            containerInsights: true,
        });

        // Create CloudWatch Log Group for ECS tasks
        const embeddingsLogGroup = new logs.LogGroup(this, 'EmbeddingsLogGroup', {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            retention: logs.RetentionDays.ONE_WEEK,
        });

        // Create Task Execution Role
        const taskExecutionRole = new iam.Role(this, 'EmbeddingsTaskExecutionRole', {
            assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName(
                    'service-role/AmazonECSTaskExecutionRolePolicy'
                ),
            ],
        });

        // Create Task Role with all the permissions the lambda had
        const taskRole = new iam.Role(this, 'EmbeddingsTaskRole', {
            assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonBedrockFullAccess'),
            ],
        });

        // Grant permissions that the lambda had
        processedAssetsBucket.grantReadWrite(taskRole); // Changed to ReadWrite for result output
        props.inputAssetsBucket.grantRead(taskRole);
        props.mediaBucket.grantReadWrite(taskRole);
        props.baseInfra.configTable.grantReadData(taskRole);
        props.rdsSecret.grantRead(taskRole);
        cacheTable.grantWriteData(taskRole);
        cacheTable.grantReadData(taskRole);
        props.linksTable.grantReadWriteData(taskRole);

        // Add Textract permissions
        taskRole.addToPolicy(
            new iam.PolicyStatement({
                actions: [
                    'textract:StartDocumentAnalysis',
                    'textract:GetDocumentAnalysis',
                    'textract:DetectDocumentText',
                    'textract:AnalyzeDocument',
                ],
                resources: ['*'],
            })
        );

        // Add Transcribe permissions for video processing
        taskRole.addToPolicy(
            new iam.PolicyStatement({
                actions: [
                    'transcribe:StartTranscriptionJob',
                    'transcribe:GetTranscriptionJob',
                    'transcribe:ListTranscriptionJobs',
                ],
                resources: ['*'],
            })
        );

        // Grant Bedrock embeddings model access
        const regionModelIds = new Map<string, Set<string>>();
        props.baseInfra.systemConfig.ragConfig.embeddingsModels.forEach((model) => {
            if (model.provider === 'bedrock') {
                const region = model.region ?? cdk.Aws.REGION;
                const modelIds = regionModelIds.get(region) ?? new Set<string>();
                modelIds.add(model.modelId);
                regionModelIds.set(region, modelIds);
            }
        });

        regionModelIds.forEach((modelIds, region) => {
            taskRole.addToPolicy(
                new iam.PolicyStatement({
                    effect: iam.Effect.ALLOW,
                    actions: [
                        'bedrock:InvokeModel',
                        'bedrock:InvokeModelWithResponseStream',
                    ],
                    resources: Array.from(modelIds).flatMap((modelId) => {
                        const isInferenceProfile = /^(us\.|eu\.|apac\.|us-gov\.)/.test(
                            modelId
                        );
                        if (isInferenceProfile) {
                            const baseModelId = modelId.replace(
                                /^(us\.|eu\.|apac\.|us-gov\.)/,
                                ''
                            );
                            return [
                                `arn:${cdk.Aws.PARTITION}:bedrock:${region}:${cdk.Aws.ACCOUNT_ID}:inference-profile/${modelId}`,
                                `arn:${cdk.Aws.PARTITION}:bedrock:*::foundation-model/${baseModelId}`,
                            ];
                        } else {
                            return `arn:${cdk.Aws.PARTITION}:bedrock:${region}::foundation-model/${modelId}`;
                        }
                    }),
                })
            );
        });

        // Grant SageMaker embeddings model access
        const endpointSet = new Set<string>();
        const sagemakerEndpoints: string[] = [];
        props.baseInfra.systemConfig.ragConfig.embeddingsModels.forEach((model) => {
            if (model.provider === 'sagemaker') {
                if (!endpointSet.has(model.modelEndpointName)) {
                    endpointSet.add(model.modelEndpointName);
                    sagemakerEndpoints.push(
                        `arn:aws:sagemaker:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:endpoint/${model.modelEndpointName}`
                    );
                }
            }
        });

        if (sagemakerEndpoints.length > 0) {
            taskRole.addToPolicy(
                new iam.PolicyStatement({
                    actions: [
                        'sagemaker:InvokeEndpoint',
                        'sagemaker:InvokeEndpointWithResponseStream',
                    ],
                    resources: sagemakerEndpoints,
                })
            );
        }

        // Create Fargate Task Definition with 8 vCPU and 32GB RAM for heavy embeddings processing
        const embeddingsTaskDefinition = new ecs.FargateTaskDefinition(
            this,
            'EmbeddingsTaskDefinition',
            {
                memoryLimitMiB: 32768, // 32 GB RAM
                cpu: 8192, // 8 vCPU
                runtimePlatform: {
                    cpuArchitecture: ecs.CpuArchitecture.ARM64,
                    operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
                },
                executionRole: taskExecutionRole,
                taskRole: taskRole,
            }
        );

        /* eslint-disable @typescript-eslint/naming-convention */
        const container = embeddingsTaskDefinition.addContainer('embeddings', {
            image: ecs.ContainerImage.fromAsset(constants.BACKEND_DIR, {
                file: path.join('ingestion', 'embeddings', 'Dockerfile.ecs'),
                platform: Platform.LINUX_ARM64,
            }),
            logging: ecs.LogDrivers.awsLogs({
                streamPrefix: 'embeddings',
                logGroup: embeddingsLogGroup,
            }),
            environment: {
                ...ingestionLambdaCommonEnvironment,
                POWERTOOLS_SERVICE_NAME: 'ingestion-embeddings',
                LINKS_TABLE_NAME: props.linksTable.tableName,
                MEDIA_BUCKET_NAME: props.mediaBucket.bucketName,
                AWS_REGION: cdk.Aws.REGION,
                REGION_NAME: cdk.Aws.REGION,
                RDS_SECRET_ARN: props.rdsSecret.secretArn,
                RDS_ENDPOINT: props.rdsEndpoint,
                CHUNK_SIZE_DOC_SPLIT: (
                    corpusConfig?.corpusProperties?.chunkingConfiguration?.chunkSize ||
                    constants.CHUNK_SIZE_DOC_SPLIT
                ).toString(),
                OVERLAP_FOR_DOC_SPLIT: (
                    corpusConfig?.corpusProperties?.chunkingConfiguration?.chunkOverlap ||
                    constants.OVERLAP_FOR_DOC_SPLIT
                ).toString(),
            },
        });
        /* eslint-enable @typescript-eslint/naming-convention */

        const vectorStoreManagementFunction = new lambda.Function(
            this,
            'vectorStoreManagementFunction',
            {
                ...ingestionLambdaCommonProps,
                code: lambda.Code.fromAsset(
                    path.join(
                        constants.BACKEND_DIR,
                        'ingestion',
                        'vector_store_management'
                    )
                ),
                environment: {
                    ...constants.LAMBDA_COMMON_ENVIRONMENT,
                    ...ingestionLambdaCommonEnvironment,

                    /* eslint-disable @typescript-eslint/naming-convention */
                    POWERTOOLS_SERVICE_NAME: 'ingestion-vector-store-management',
                    RDS_SECRET_ARN: props.rdsSecret.secretArn,
                    RDS_ENDPOINT: props.rdsEndpoint,
                    CHUNK_SIZE_DOC_SPLIT: constants.CHUNK_SIZE_DOC_SPLIT,
                    OVERLAP_FOR_DOC_SPLIT: constants.OVERLAP_FOR_DOC_SPLIT,

                    /* eslint-enable @typescript-eslint/naming-convention */
                },
            }
        );
        props.rdsSecret.grantRead(vectorStoreManagementFunction);
        cacheTable.grantReadWriteData(vectorStoreManagementFunction);

        // Step function definition
        const inputValidationTask = new stepfn_task.LambdaInvoke(
            this,
            'Detect and identify documents for ingestion',
            {
                lambdaFunction: inputValidationFunction,
                payload: stepfn.TaskInput.fromObject(startState),
                resultSelector: {
                    // eslint-disable-next-line @typescript-eslint/naming-convention
                    'Payload.$': '$.Payload',
                },
            }
        );

        // Replace Lambda invoke with ECS run task
        const embeddingsTask = new stepfn_task.EcsRunTask(
            this,
            'Generate embeddings from processed documents and store them',
            {
                integrationPattern: stepfn.IntegrationPattern.RUN_JOB,
                cluster: embeddingsCluster,
                taskDefinition: embeddingsTaskDefinition,
                assignPublicIp: false,
                containerOverrides: [
                    {
                        containerDefinition: container,
                        environment: [
                            {
                                name: 'INPUT_JSON',
                                value: stepfn.JsonPath.jsonToString(
                                    stepfn.JsonPath.entirePayload
                                ),
                            },
                            {
                                name: 'EXECUTION_NAME',
                                value: stepfn.JsonPath.stringAt('$$.Execution.Name'),
                            },
                            {
                                name: 'STATE_MACHINE_NAME',
                                value: stepfn.JsonPath.stringAt('$$.StateMachine.Name'),
                            },
                        ],
                    },
                ],
                launchTarget: new stepfn_task.EcsFargateLaunchTarget({
                    platformVersion: ecs.FargatePlatformVersion.LATEST,
                }),
                subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
            }
        );

        const vectorStoreManagementTask = new stepfn_task.LambdaInvoke(
            this,
            'Vector store management task',
            {
                lambdaFunction: vectorStoreManagementFunction,
                payload: stepfn.TaskInput.fromJsonPathAt('$$.Execution.Input'),
                resultPath: stepfn.JsonPath.DISCARD,
            }
        );

        const ingestionChoice = new stepfn.Choice(
            this,
            'Are there any documents that need to be ingested?'
        );

        // Update DistributedMap to use 1 concurrent task as requested
        const runFilesInParallel = new stepfn.DistributedMap(this, 'DistributedMap', {
            maxConcurrency: 1, // Changed to 1 as requested
            itemReader: new stepfn.S3JsonItemReader({
                bucket: processedAssetsBucket,
                key: stepfn.JsonPath.format(
                    'ingestion_input/{}/config.json',
                    stepfn.JsonPath.stringAt('$$.Execution.Name')
                ),
            }),
            resultWriter: new stepfn.ResultWriter({
                bucket: processedAssetsBucket,
                prefix: stepfn.JsonPath.format(
                    'ingestion_output/{}/sf-results',
                    stepfn.JsonPath.stringAt('$$.Execution.Name')
                ),
            }),
        }).itemProcessor(embeddingsTask);

        const succeedTask = new stepfn.Succeed(this, 'Succeed');

        const definition = inputValidationTask.next(
            vectorStoreManagementTask.next(
                ingestionChoice
                    .when(
                        stepfn.Condition.booleanEquals('$.Payload.isValid', false),
                        succeedTask
                    )
                    .otherwise(runFilesInParallel.next(succeedTask))
            )
        );

        const logGroup = new logs.LogGroup(this, 'IngestionStateMachineLogGroup');

        const ingestionStateMachine = new stepfn.StateMachine(
            this,
            'IngestionStateMachine',
            {
                definitionBody: stepfn.DefinitionBody.fromChainable(definition),
                tracingEnabled: true,
                logs: {
                    destination: logGroup,
                    level: stepfn.LogLevel.ALL,
                },
            }
        );

        // Grant Step Functions permission to run ECS tasks
        ingestionStateMachine.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ['ecs:RunTask', 'ecs:StopTask', 'ecs:DescribeTasks'],
                resources: [
                    embeddingsTaskDefinition.taskDefinitionArn,
                    `arn:aws:ecs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:task/${embeddingsCluster.clusterName}/*`,
                ],
            })
        );

        ingestionStateMachine.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ['iam:PassRole'],
                resources: [taskRole.roleArn, taskExecutionRole.roleArn],
            })
        );

        new cdk.CfnOutput(this, 'StateMachineArn', {
            value: ingestionStateMachine.stateMachineArn,
        });

        new cdk.CfnOutput(this, 'EmbeddingsClusterArn', {
            value: embeddingsCluster.clusterArn,
        });

        this.applyNagSuppressions();

        this.ingestionStateMachine = ingestionStateMachine;
        this.embeddingsCluster = embeddingsCluster;
        this.embeddingsTaskDefinition = embeddingsTaskDefinition;
        this.inputValidationFunction = inputValidationFunction;
    }

    private applyNagSuppressions(): void {
        const stack = cdk.Stack.of(this);

        [
            'IngestionPipeline/IngestionStateMachine/Role/DefaultPolicy/Resource',
            'IngestionPipeline/inputValidationFunction/ServiceRole/DefaultPolicy/Resource',
            'IngestionPipeline/inputValidationFunction/ServiceRole/Resource',
            'IngestionPipeline/cacheUpdateFunction/ServiceRole/DefaultPolicy/Resource',
            'IngestionPipeline/cacheUpdateFunction/ServiceRole/Resource',
            'IngestionPipeline/IngestionStateMachine/DistributedMapPolicy/Resource',
            'IngestionPipeline/vectorStoreManagementFunction/ServiceRole/Resource',
            'IngestionPipeline/vectorStoreManagementFunction/ServiceRole/DefaultPolicy/Resource',
            'IngestionPipeline/EmbeddingsTaskRole/Resource',
            'IngestionPipeline/EmbeddingsTaskRole/DefaultPolicy/Resource',
            'IngestionPipeline/EmbeddingsTaskExecutionRole/Resource',
            'IngestionPipeline/EmbeddingsTaskExecutionRole/DefaultPolicy/Resource',
            'BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource',
            'BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/DefaultPolicy/Resource',
        ].forEach((p) => {
            NagSuppressions.addResourceSuppressionsByPath(
                stack,
                `${stack.stackName}/${p}`,
                [
                    {
                        id: 'AwsSolutions-IAM4',
                        reason: 'The only managed policy that is used is the AWSLambdaBasicExecutionRole which is provided by default by CDK',
                    },
                    {
                        id: 'AwsSolutions-IAM5',
                        reason: 'CDK deployment resources are managed by CDK',
                    },
                ]
            );
        });

        // Add specific suppressions for Lambda runtime versions
        [
            'IngestionPipeline/inputValidationFunction/Resource',
            'IngestionPipeline/cacheUpdateFunction/Resource',
            'IngestionPipeline/vectorStoreManagementFunction/Resource',
        ].forEach((p) => {
            NagSuppressions.addResourceSuppressionsByPath(
                stack,
                `${stack.stackName}/${p}`,
                [
                    {
                        id: 'AwsSolutions-L1',
                        reason: 'The selected runtime version, Python 3.11, has been intentionally chosen to align with specific project requirements',
                    },
                ]
            );
        });

        // Add suppression for ECS Task Definition environment variables
        NagSuppressions.addResourceSuppressionsByPath(
            stack,
            `${stack.stackName}/IngestionPipeline/EmbeddingsTaskDefinition/Resource`,
            [
                {
                    id: 'AwsSolutions-ECS2',
                    reason: 'Environment variables are necessary for the embeddings processing task to function correctly and contain non-sensitive configuration data',
                },
            ]
        );
    }
}
