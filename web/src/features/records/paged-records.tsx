import { useMemo } from "react";
import { ClockIcon, DownloadIcon, TriangleAlertIcon } from "lucide-react";

import { IconButton } from "@/components/icon-button";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { useDebouncedValue } from "@/hooks/use-debounced-value";
import { recordsExportUrl, useRecords, type RecordsFilter } from "@/lib/api/live";
import { apiErrorMessage } from "@/lib/api/client";
import type { KafkaRecord, RecordOrder, TopicDetail } from "@/lib/api/types";
import { fromDatetimeLocalValue } from "@/lib/format";

import { RecordModeSwitch, type RecordMode } from "./record-mode";
import { RecordView, filterPartitions, type RecordFilter, type RecordSource } from "./record-view";
import { useTimestampFilter } from "./timestamp-filter";
import { recordId } from "./record-id";

const EMPTY_RECORDS: KafkaRecord[] = [];

type PagedRecordsProps = {
  cluster: string;
  topic: TopicDetail;
  filter: RecordFilter;
  onFilterChange: (filter: RecordFilter) => void;
  order: RecordOrder;
  onModeChange: (mode: RecordMode) => void;
};

export function PagedRecords({
  cluster,
  topic,
  filter,
  onFilterChange,
  order,
  onModeChange,
}: PagedRecordsProps) {
  const timestamp = useTimestampFilter();
  const { from, to } = timestamp.range;

  const needle = useDebouncedValue(filter.term.trim());
  const partitions = useMemo(() => filterPartitions(topic, filter), [topic, filter]);

  const query = useMemo<RecordsFilter>(
    () => ({
      topic: topic.name,
      partitions,
      order,
      from: fromDatetimeLocalValue(from),
      to: fromDatetimeLocalValue(to),
      filter: needle ? { contains: needle } : null,
      schemaId: filter.schemaId,
    }),
    [topic.name, partitions, filter.schemaId, order, from, to, needle],
  );

  const {
    data,
    isFetching,
    isFetchingNextPage,
    isFetchNextPageError,
    isPlaceholderData,
    isError,
    error,
    fetchNextPage,
    hasNextPage,
  } = useRecords(cluster, query);

  const records = useMemo(() => {
    const pages = data?.pages;
    if (!pages?.length) return EMPTY_RECORDS;

    const seen = new Set<string>();
    const rows: KafkaRecord[] = [];
    for (const page of pages) {
      for (const record of page.records) {
        const id = recordId(record);
        if (seen.has(id)) continue;
        seen.add(id);
        rows.push(record);
      }
    }
    return rows;
  }, [data?.pages]);
  const lastPage = data?.pages[data.pages.length - 1];

  const source: RecordSource = {
    records,
    scope: JSON.stringify(query),
    obfuscated: data?.pages.some((page) => page.obfuscated) ?? false,
    loading: isFetching && !isFetchingNextPage && records.length === 0,
    refreshing: isFetching && !isFetchingNextPage && records.length > 0,
    stale: isPlaceholderData,
    error: isError ? apiErrorMessage(error, "Failed to load records.") : undefined,
    pages: {
      hasNextPage: Boolean(hasNextPage) && !isPlaceholderData,
      fetchNextPage: () => void fetchNextPage(),
      isFetchingNextPage,
      isFetchNextPageError,
    },
  };

  return (
    <RecordView
      cluster={cluster}
      topic={topic}
      source={source}
      filter={filter}
      onFilterChange={onFilterChange}
      actions={
        <>
          {partitions?.length !== 0 ? (
            <IconButton
              label="Download matching records as NDJSON"
              tooltip="Download NDJSON"
              size="icon"
              nativeButton={false}
              render={<a href={recordsExportUrl(cluster, query)} download />}
            >
              <DownloadIcon className="size-3.5" />
            </IconButton>
          ) : null}
          <RecordModeSwitch value={order} onChange={onModeChange} />
        </>
      }
      notice={
        lastPage && !lastPage.complete && !isPlaceholderData ? (
          <Alert>
            <TriangleAlertIcon />
            <AlertTitle>Partial scan</AlertTitle>
            <AlertDescription>
              The scan timed out before it reached every offset, so more matches may exist. Scroll
              to keep scanning.
            </AlertDescription>
          </Alert>
        ) : null
      }
      filters={[timestamp.filter]}
      emptyState={
        <Empty className="py-10">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <ClockIcon />
            </EmptyMedia>
            <EmptyTitle>No records</EmptyTitle>
            <EmptyDescription>
              {from || to
                ? "Nothing in the selected time range."
                : filter.term
                  ? "Nothing matched your search in the scanned offsets."
                  : partitions
                    ? "Nothing in the selected partitions."
                    : "This topic has no records."}
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      }
    />
  );
}
