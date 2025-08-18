/*
Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import { Construct } from 'constructs';

export interface LangchainProps {
    readonly removalPolicy?: cdk.RemovalPolicy;
    readonly license?: string;
    readonly layerVersionName?: string;
    readonly description?: string;
}

export interface LayerProps extends LangchainProps {
    runtime: lambda.Runtime;
    architecture: lambda.Architecture;
    path: string;
    autoUpgrade?: boolean;
    additionalPackages?: string[];
    local?: 'python' | 'python3';
}

export class Layer extends Construct {
    public layer: lambda.LayerVersion;

    public constructor(scope: Construct, id: string, props: LayerProps) {
        super(scope, id);

        const { runtime, architecture, path, additionalPackages, autoUpgrade, local } =
            props;

        const args = local ? [] : ['-t /asset-output/python'];
        if (additionalPackages) {
            args.push(...additionalPackages);
        }
        if (autoUpgrade) {
            args.push('--upgrade');
        }

        const layerAsset = new s3assets.Asset(this, 'LayerAsset', {
            path,
            bundling: {
                image: cdk.DockerImage.fromRegistry(
                    'public.ecr.aws/sam/build-python3.11'
                ),
                command: [
                    'sh',
                    '-c',
                    [
                        'set -ex',
                        'mkdir -p /tmp/layer/python',
                        'pip install --no-cache-dir -r /asset-input/requirements.txt -t /tmp/layer/python',
                        'cd /tmp/layer',
                        'zip -r layer.zip .',
                        'mkdir -p /asset-output && rm -rf /asset-output/*',
                        'mv layer.zip /asset-output/',
                    ].join(' && '),
                ],
                outputType: cdk.BundlingOutput.ARCHIVED,
                workingDirectory: '/asset-input',
            },
        });

        const layer = new lambda.LayerVersion(this, 'Layer', {
            code: lambda.Code.fromBucket(layerAsset.bucket, layerAsset.s3ObjectKey),
            compatibleRuntimes: [runtime],
            compatibleArchitectures: [architecture],
            ...props,
        });

        this.layer = layer;
    }
}
