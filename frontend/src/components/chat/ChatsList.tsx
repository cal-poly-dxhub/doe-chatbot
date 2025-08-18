/*
Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: LicenseRef-.amazon.com.-AmznSL-1.0
Licensed under the Amazon Software License http://aws.amazon.com/asl/
*/
import { useCollection } from '@cloudscape-design/collection-hooks';
import Box from '@cloudscape-design/components/box';
import Button from '@cloudscape-design/components/button';
import Cards from '@cloudscape-design/components/cards';
import Header from '@cloudscape-design/components/header';
import Pagination from '@cloudscape-design/components/pagination';
import TextFilter from '@cloudscape-design/components/text-filter';
import { useCreateChatMutation } from '../../hooks/chats';
import { Chat } from '../../react-query-hooks/models/Chat';
import EmptyState from '../Empty';
import { getMatchesCountText } from '../table-config';
import { useMemo, useState } from 'react';

type ChatCardsProps = {
  items: Chat[];
  selectedItem: Chat | undefined;
  onSelect: (chat: Chat) => void;
  loading: boolean;
};

const definitions = [
  {
    id: 'createdAt',
    content: (item: Chat) => item.createdAt && new Date(item.createdAt).toLocaleString(),
  },
];

function ChatCards({ items: allChats, selectedItem, onSelect, loading }: ChatCardsProps) {
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const sortedChats = useMemo(() => {
    const ts = (d: number | undefined) => d ?? 0; // 0 ⇒ epoch fallback

    return [...allChats].sort((a, b) =>
      sortDirection === 'asc' ? ts(a.createdAt) - ts(b.createdAt) : ts(b.createdAt) - ts(a.createdAt),
    );
  }, [allChats, sortDirection]);

  const preferences = {
    pageSize: 4,
    visibleContent: ['title', 'createdAt'] as readonly string[],
  };

  const { items, actions, filteredItemsCount, collectionProps, filterProps, paginationProps } = useCollection(
    sortedChats,
    {
      filtering: {
        empty: <EmptyState title="No chats" subtitle="No chats to display." action={<Button>Create chat</Button>} />,
        noMatch: (
          <EmptyState
            title="No matches"
            subtitle="We can’t find a match."
            action={<Button onClick={() => actions.setFiltering('')}>Clear filter</Button>}
          />
        ),
      },
      pagination: { pageSize: preferences.pageSize },
      sorting: {},
      selection: {},
    },
  );

  const createChat = useCreateChatMutation((response) => {
    onSelect(response);
  });

  async function createNewChat() {
    await createChat.mutateAsync({
      createChatRequestContent: {
        title: 'New Chat - ' + new Date().toLocaleString(),
      },
    });
  }

  return (
    <Cards
      {...collectionProps}
      onSelectionChange={({ detail }) => {
        onSelect(detail.selectedItems[0]);
      }}
      stickyHeader
      loading={loading}
      selectedItems={selectedItem ? [selectedItem] : []}
      selectionType="single"
      ariaLabels={{
        itemSelectionLabel: (_e, n) => `select ${n.title}`,
        selectionGroupLabel: 'Chat selection',
      }}
      cardDefinition={{
        header: (item) => <span style={{ fontSize: '13pt' }}>{item.title}</span>,
        sections: definitions,
      }}
      cardsPerRow={[{ cards: 1 }, { minWidth: 300, cards: 1 }]}
      items={items}
      loadingText="Loading chats"
      empty={
        <Box textAlign="center" color="inherit">
          <b>No chats</b>
          <Box padding={{ bottom: 's' }} variant="p" color="inherit">
            No chats to display.
          </Box>
        </Box>
      }
      header={
        <Header
          actions={
            <Button
              variant="primary"
              iconName="add-plus"
              onClick={createNewChat}
              loading={loading || createChat.isLoading}
            >
              Create Chat
            </Button>
          }
        >
          Chats
        </Header>
      }
      pagination={<Pagination {...paginationProps} />}
      filter={
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%' }}>
          <TextFilter
            {...filterProps}
            countText={getMatchesCountText(filteredItemsCount || 0)}
            filteringAriaLabel="Filter chats"
          />

          <Button
            iconName={sortDirection === 'asc' ? 'angle-up' : 'angle-down'}
            variant="icon"
            onClick={() => setSortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'))}
            ariaLabel="Toggle sort direction"
          />
        </div>
      }
    />
  );
}

export default ChatCards;
