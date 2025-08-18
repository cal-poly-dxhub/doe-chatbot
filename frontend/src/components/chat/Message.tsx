/*
Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: LicenseRef-.amazon.com.-AmznSL-1.0
Licensed under the Amazon Software License http://aws.amazon.com/asl/
*/
import React, { useState, useRef, useEffect } from 'react';
import { Button, TextContent, Box, Modal, SpaceBetween, Spinner } from '@cloudscape-design/components';
import { SourceDocument } from './components/SourceDocument';
import { useDeleteChatMessageMutation, useMessageSources } from '../../hooks/chats';
import type { ChatMessage } from '../../react-query-hooks';
import CopyText from '../buttons/CopyText';
import { ModalButton } from '../buttons/ModalButton';
import FeedbackPanel from './FeedbackPanel';

type MessageProps = {
  message: ChatMessage;
  humanStyles?: React.HTMLAttributes<HTMLDivElement>['style'];
  aiStyles?: React.HTMLAttributes<HTMLDivElement>['style'];
};

type MessageSourcesProps = {
  messageId: string;
  chatId: string;
};

const ImageRenderer = ({ url }: { url: string }) => {
  return (
    <div className="message-media">
      <img src={url} alt="Assistant provided content" style={{ maxWidth: '100%', borderRadius: '4px' }} />
    </div>
  );
};

const VideoRenderer = ({ url, timestamp }: { url: string; timestamp?: number }) => {
  const videoRef = useRef<HTMLVideoElement>(null);

  // Set the current time when the timestamp changes or the video loads
  useEffect(() => {
    if (videoRef.current && timestamp !== undefined) {
      // Set the current time when the video is loaded
      const handleLoadedMetadata = () => {
        if (videoRef.current) {
          videoRef.current.currentTime = timestamp;
        }
      };

      // Add event listener for metadata loaded
      videoRef.current.addEventListener('loadedmetadata', handleLoadedMetadata);

      // If video is already loaded, set time immediately
      if (videoRef.current.readyState >= 2) {
        videoRef.current.currentTime = timestamp;
      }

      // Clean up event listener
      return () => {
        if (videoRef.current) {
          videoRef.current.removeEventListener('loadedmetadata', handleLoadedMetadata);
        }
      };
    }

    // Return an empty cleanup function if condition is not met
    return () => {};
  }, [timestamp]);

  return (
    <div className="message-media">
      <video ref={videoRef} controls style={{ maxWidth: '100%', borderRadius: '4px' }}>
        <source src={url} type="video/mp4" />
        Your browser does not support the video tag.
      </video>
      {timestamp !== undefined && (
        <div style={{ fontSize: '0.7em', color: '#777', marginTop: '2px' }}>Timestamp: {formatTime(timestamp)}</div>
      )}
    </div>
  );
};

// Helper function to format seconds as mm:ss
const formatTime = (seconds: number): string => {
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return `${mins}:${secs.toString().padStart(2, '0')}`;
};

// Message content parser function
const parseMessageContent = (text: string): React.ReactNode[] => {
  if (!text) return [text];

  const parts: React.ReactNode[] = [];

  // Regular expressions to match media patterns
  const imagePattern = /\(!\?#Image:(https:\/\/[^)]+)\)/g;
  const videoPattern = /\(!\?#Video:(https:\/\/[^)]+)\)/g;
  const timestampPattern = /\(!\?#Timestamp:(\d+\.?\d*)\)/g;

  // Create a copy of text we'll work with
  let allMatches: { start: number; end: number; node: React.ReactNode }[] = [];
  let timestamp: number | undefined;

  // Extract timestamp if present
  let timestampMatch;
  while ((timestampMatch = timestampPattern.exec(text)) !== null) {
    timestamp = parseFloat(timestampMatch[1]);
    // Mark the timestamp pattern for removal
    allMatches.push({
      start: timestampMatch.index,
      end: timestampMatch.index + timestampMatch[0].length,
      node: <React.Fragment key={`timestamp-${timestampMatch.index}`} />,
    });
  }

  // Find all video matches and apply timestamp if present
  let videoMatch;
  while ((videoMatch = videoPattern.exec(text)) !== null) {
    const url = videoMatch[1];
    allMatches.push({
      start: videoMatch.index,
      end: videoMatch.index + videoMatch[0].length,
      node: <VideoRenderer key={`vid-${videoMatch.index}`} url={url} timestamp={timestamp} />,
    });
  }

  // Find all image matches
  let imageMatch;
  while ((imageMatch = imagePattern.exec(text)) !== null) {
    const url = imageMatch[1];
    allMatches.push({
      start: imageMatch.index,
      end: imageMatch.index + imageMatch[0].length,
      node: <ImageRenderer key={`img-${imageMatch.index}`} url={url} />,
    });
  }

  // Sort matches by their position in the text
  allMatches.sort((a, b) => a.start - b.start);

  // Build the result by combining text segments and media components
  let lastIndex = 0;
  for (const match of allMatches) {
    // Add text before this match
    if (match.start > lastIndex) {
      parts.push(text.substring(lastIndex, match.start));
    }

    // Add the media component or empty fragment for timestamp
    parts.push(match.node);

    lastIndex = match.end;
  }

  // Add any remaining text
  if (lastIndex < text.length) {
    parts.push(text.substring(lastIndex));
  }

  return parts.length > 0 ? parts : [text];
};

export function MessageSources({ chatId, messageId }: MessageSourcesProps) {
  const sourcesRequest = useMessageSources(chatId, messageId);
  return (
    <div>
      {sourcesRequest.isLoading ? (
        <Spinner size="big" />
      ) : (
        sourcesRequest.data &&
        (sourcesRequest.data.length ? (
          <SpaceBetween direction="vertical" size="m">
            {sourcesRequest.data.map((source, i) => (
              <SourceDocument key={source.sourceId} title={`Source #${i + 1}`} document={source} />
            ))}
          </SpaceBetween>
        ) : (
          'No sources available'
        ))
      )}
    </div>
  );
}

function SourcePopover(props: { chatId: string; messageId: string }) {
  const [visible, setVisible] = useState(false);
  return (
    <>
      {/* Lazy load sources */}
      {visible && (
        <Modal onDismiss={() => setVisible(false)} visible size="large" header="Sources">
          <MessageSources chatId={props.chatId} messageId={props.messageId} />
        </Modal>
      )}

      <Button iconName="folder-open" variant="inline-icon" onClick={() => setVisible(true)} />
    </>
  );
}

export default function Message({ message, humanStyles = {}, aiStyles = {} }: MessageProps) {
  console.log('Rendering message with type: ', message.type, message.text);
  const [deleteChatMessageModalVisible, setDeleteChatMessageModalVisiblity] = useState(false);

  const deleteChatMessageMutation = useDeleteChatMessageMutation(() => {
    setDeleteChatMessageModalVisiblity(false);
  });

  async function deleteChatMessage() {
    await deleteChatMessageMutation.mutateAsync({
      chatId: message.chatId,
      messageId: message.messageId,
    });
  }

  function confirmDeleteChatMessage() {
    setDeleteChatMessageModalVisiblity(true);
  }

  let headerText = message.type === 'human' ? 'You' : 'Assistant';
  const time = new Date(message.createdAt).toLocaleString();
  // TODO: once we persist trace data we can handle this properly; added to ai messages in chat hooks
  const traceData = (message as any).traceData;

  // Parse message content to handle media elements
  const parsedContent = parseMessageContent(message.text);
  console.log('Message Type: ', message.type);
  return (
    <div
      style={{
        padding: '15px 10px',
        ...(message.type === 'ai' ? aiStyles : humanStyles),
      }}
    >
      <Modal
        onDismiss={() => setDeleteChatMessageModalVisiblity(false)}
        visible={deleteChatMessageModalVisible}
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteChatMessageModalVisiblity(false)}>
                Cancel
              </Button>
              <Button
                loading={deleteChatMessageMutation.isLoading}
                disabled={deleteChatMessageMutation.isLoading || deleteChatMessageMutation.isError}
                variant="primary"
                onClick={() => deleteChatMessage()}
              >
                Ok
              </Button>
            </SpaceBetween>
          </Box>
        }
        header="Confirm chat message deletion"
      >
        <p>
          Are you sure you want to delete this chat message? <br />
          This operation will only delete this specific message and can not be undone.
        </p>
      </Modal>
      <TextContent>
        <div
          style={{
            display: 'flex',
            flexDirection: 'row',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '8px',
            gap: '8px',
            width: '100%',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'flex-start', gap: '8px' }}>
            <h4 style={{ margin: 0 }}>{headerText}</h4>
            {message.type === 'ai' && <FeedbackPanel messageId={message.messageId} chatId={message.chatId} />}
          </div>
          <span>
            {traceData && (
              <ModalButton
                button={{ variant: 'inline-icon', iconName: 'bug' }}
                modal={{
                  header: 'Trace Data',
                  size: 'max',
                  footer: 'Trace data is only available locally and not persistent',
                }}
              >
                <code style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(traceData, null, 2)}</code>
              </ModalButton>
            )}
            <span style={{ color: '#aaa' }}>{time}</span>
          </span>
        </div>
      </TextContent>
      <div
        style={{
          display: 'flex',
          flexDirection: 'row',
          justifyContent: 'space-between',
        }}
      >
        <TextContent>
          {/* Replace the original content rendering with our parsed content */}
          <div style={{ whiteSpace: 'pre-wrap' }}>{parsedContent}</div>
        </TextContent>
        <div
          style={{
            display: 'flex',
            flexDirection: 'row',
            justifyContent: 'flex-end',
            alignItems: 'flex-end',
          }}
        >
          {message.type === 'ai' && <SourcePopover chatId={message.chatId} messageId={message.messageId} />}
          <CopyText textToCopy={message.text} contentName="Message" />
          <Button
            iconName="delete-marker"
            variant="inline-icon"
            onClick={() => {
              confirmDeleteChatMessage();
            }}
          />
        </div>
      </div>
    </div>
  );
}
