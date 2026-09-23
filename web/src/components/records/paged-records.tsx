import { useMemo, useState } from "react";
import { ClockIcon, TriangleAlertIcon } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import { RecordModeSwitch, type RecordMode } from "@/components/records/record-mode";
import { RecordView, type RecordFilter, type RecordSource } from "@/components/records/record-view";
import { useDebouncedValue } from "@/hooks/use-debounced-value";
import { useRecords, type RecordsFilter } from "@/lib/api/live";
import { apiErrorMessage } from "@/lib/api/client";
import type { KafkaRecord, RecordOrder, TopicDetail } from "@/lib/api/types";
import { fromDatetimeLocalValue } from "@/lib/format";
import { recordId } from "@/lib/records";
import { cn } from "@/lib/utils";

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
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");

  const needle = useDebouncedValue(filter.term.trim());

  const query = useMemo<RecordsFilter>(
    () => ({
      topic: topic.name,
      partition: filter.partition,
      order,
      from: fromDatetimeLocalValue(from),
      to: fromDatetimeLocalValue(to),
      filter: needle ? { contains: needle } : null,
      schemaId: filter.schemaId,
    }),
    [topic.name, filter.partition, filter.schemaId, order, from, to, needle],
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
      actions={<RecordModeSwitch value={order} onChange={onModeChange} />}
      notice={
        lastPage && !lastPage.complete && !isPlaceholderData ? (
          <Alert>
            <TriangleAlertIcon />
            <AlertTitle>Partial scan</AlertTitle>
            <AlertDescription>
              The scan timed out before it read every matching offset. These records match. Keep
              scrolling to continue.
            </AlertDescription>
          </Alert>
        ) : null
      }
      controls={
        <InputGroup className="w-auto bg-background dark:bg-input/20">
          <InputGroupAddon>
            <ClockIcon className="size-3.5!" />
          </InputGroupAddon>
          <InputGroupInput
            type="datetime-local"
            value={from}
            max={to || undefined}
            onChange={(event) => setFrom(event.target.value)}
            aria-label="From timestamp"
            className={cn("w-44 pr-1", !from && "text-muted-foreground")}
          />
          <span aria-hidden className="text-muted-foreground/60">
            →
          </span>
          <InputGroupInput
            type="datetime-local"
            value={to}
            min={from || undefined}
            onChange={(event) => setTo(event.target.value)}
            aria-label="To timestamp"
            className={cn("w-44 pl-2", !to && "text-muted-foreground")}
          />
        </InputGroup>
      }
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
                  : "This topic has no records."}
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      }
    />
  );
}
