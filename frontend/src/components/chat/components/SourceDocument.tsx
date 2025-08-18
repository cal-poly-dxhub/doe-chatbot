/*
Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: LicenseRef-.amazon.com.-AmznSL-1.0
Licensed under the Amazon Software License http://aws.amazon.com/asl/
*/
import { Box, Container, ExpandableSection, Grid, Header, TextContent, Link } from '@cloudscape-design/components';
import { FC } from 'react';
import { Document } from '../../../react-query-hooks';

export const SourceDocument: FC<{ title: string; document: Document }> = ({ title, document }) => {
  const scoreText = document.score != null ? `Score: ${document.score} (distance)` : undefined;
  const sourceUrl = document.metadata?.source?.startsWith('http') ? document.metadata.source : undefined;

  // Format metadata to show only the object key for source
  const formattedMetadata = { ...document.metadata };
  if (formattedMetadata.source) {
    try {
      const url = new URL(formattedMetadata.source);
      formattedMetadata.source = url.pathname.substring(1); // Remove leading slash
    } catch (e) {
      // If not a valid URL, keep as is
    }
  }

  return (
    <Container
      header={
        <Header variant="h3" description={scoreText}>
          {title}
        </Header>
      }
      footer={
        <ExpandableSection headerText="Metadata" variant="footer">
          <Grid>
            {Object.entries(formattedMetadata).map(([label, value], i) => (
              <div key={i}>
                <Box variant="awsui-key-label">{label}</Box>
                <div>{String(value)}</div>
              </div>
            ))}
          </Grid>
        </ExpandableSection>
      }
    >
      <TextContent>
        {sourceUrl && (
          <Link href={sourceUrl} target="_blank" external>
            View Source Document
          </Link>
        )}
      </TextContent>
    </Container>
  );
};
