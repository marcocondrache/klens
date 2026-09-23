import { useCallback, useMemo, useState } from "react";
import { ClockIcon, EyeOffIcon, TriangleAlertIcon } from "lucide-react";
import { Link } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { type DataTableFeatures } from "@/components/data-table/features";
import { RecordTable } from "@/components/records/record-table";
import { PayloadView } from "@/components/payload-view";
import { SchemaPicker } from "@/components/schema-picker";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useSubjectRows } from "@/lib/api/catalog";
import { useRecords, type RecordsFilter } from "@/lib/api/live";
import { useAccess } from "@/hooks/use-access";
import { useDebouncedValue } from "@/hooks/use-debounced-value";
import { apiErrorMessage } from "@/lib/api/client";
import { formatBytes, formatRelative, formatTimestamp, fromDatetimeLocalValue } from "@/lib/format";
import type { KafkaRecord, RecordOrder, TopicDetail } from "@/lib/api/types";
import { cn } from "@/lib/utils";

const ORDER_ITEMS = [
  { value: "NEWEST", label: "Newest" },
  { value: "OLDEST", label: "Oldest" },
] as const;

const EMPTY_RECORDS: KafkaRecord[] = [];

function preview(value: string | null) {
  if (!value) return "—";
  return value.replace(/\s+/g, " ").trim();
}

function ObfuscatedBadge() {
  return (
    <Tooltip>
      <TooltipTrigger render={<Pill tone="brand" className="cursor-default" />}>
        <EyeOffIcon className="size-3.5" />
        Obfuscated
      </TooltipTrigger>
      <TooltipContent className="block max-w-80 py-2 leading-relaxed">
        A rule on this cluster hides fields on this topic. Protected fields show as *** or as kx:
        tokens. The same value always gets the same token. A masked number shows as text. A value
        the schema registry could not decode is hidden entirely. Search matches this masked view,
        not the original bytes.
      </TooltipContent>
    </Tooltip>
  );
}

const columnHelper = createColumnHelper<DataTableFeatures, KafkaRecord>();

const columns = columnHelper.columns([
  columnHelper.accessor("partition", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Part" className="justify-end" />
    ),
    meta: { align: "right", headerClassName: "w-16" },
    cell: ({ getValue }) => <span className="numeric text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Offset" className="justify-end" />
    ),
    meta: { align: "right", headerClassName: "w-28" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Key" />,
    cell: ({ row }) => (
      <span
        className={cn(
          "block max-w-48 truncate font-mono",
          row.original.key == null && "text-muted-foreground/60 italic",
        )}
      >
        {row.original.key ?? "null"}
      </span>
    ),
  }),
  columnHelper.accessor((record) => record.value ?? "", {
    id: "value",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Value" />,
    cell: ({ row }) => (
      <span className="block truncate font-mono text-muted-foreground">
        {preview(row.original.value)}
      </span>
    ),
  }),
  columnHelper.accessor("sizeBytes", {
    id: "size",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Size" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <span className="numeric text-muted-foreground">{formatBytes(getValue())}</span>
    ),
  }),
  columnHelper.accessor("timestamp", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Timestamp" className="justify-end" />
    ),
    meta: { align: "right" },
    sortFn: "datetime",
    cell: ({ getValue }) => (
      <Tooltip>
        <TooltipTrigger
          render={
            <span className="numeric cursor-default whitespace-nowrap text-muted-foreground" />
          }
        >
          {formatTimestamp(getValue())}
        </TooltipTrigger>
        <TooltipContent>{formatRelative(getValue())}</TooltipContent>
      </Tooltip>
    ),
  }),
]);

export function RecordBrowser({ cluster, topic }: { cluster: string; topic: TopicDetail }) {
  const [partition, setPartition] = useState<string>("all");
  const [term, setTerm] = useState("");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [order, setOrder] = useState<RecordOrder>("NEWEST");
  const [selected, setSelected] = useState<KafkaRecord | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [schemaId, setSchemaId] = useState<number | null>(null);

  const { can } = useAccess();

  const needle = useDebouncedValue(term.trim());

  const query = useMemo<RecordsFilter>(
    () => ({
      topic: topic.name,
      partition: partition === "all" ? null : Number(partition),
      order,
      from: fromDatetimeLocalValue(from),
      to: fromDatetimeLocalValue(to),
      filter: needle ? { contains: needle } : null,
      schemaId,
    }),
    [topic.name, partition, order, from, to, needle, schemaId],
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
  } = useRecords(cluster, query, can(cluster, "RECORDS"));
  const loadMore = useCallback(() => {
    void fetchNextPage();
  }, [fetchNextPage]);

  const records = useMemo(() => {
    const pages = data?.pages;
    if (!pages?.length) return EMPTY_RECORDS;

    const seen = new Set<string>();
    const rows: KafkaRecord[] = [];
    for (const page of pages) {
      for (const record of page.records) {
        const id = `${record.partition}-${record.offset}`;
        if (seen.has(id)) continue;
        seen.add(id);
        rows.push(record);
      }
    }
    return rows;
  }, [data?.pages]);
  const lastPage = data?.pages[data.pages.length - 1];
  const scanKey = JSON.stringify(query);
  const obfuscated = data?.pages.some((page) => page.obfuscated) ?? false;
  const showSchemaPicker =
    schemaId != null || records.some((record) => record.value != null && record.schemaId == null);
  const selectedRecord =
    selected == null
      ? null
      : (records.find(
          (record) => record.partition === selected.partition && record.offset === selected.offset,
        ) ?? selected);
  // A framed value names its own schema; the picker's override only reads the
  // values that carry none.
  const selectedSchemaId =
    selectedRecord?.value == null ? null : (selectedRecord.schemaId ?? schemaId);

  const partitionItems = [
    { value: "all", label: "All partitions" },
    ...topic.partitions.map((part) => ({
      value: String(part.id),
      label: `Partition ${part.id}`,
    })),
  ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {lastPage && !lastPage.complete && !isPlaceholderData ? (
        <Alert>
          <TriangleAlertIcon />
          <AlertTitle>Partial scan</AlertTitle>
          <AlertDescription>
            The scan timed out before it read every matching offset. These records match. Keep
            scrolling to continue.
          </AlertDescription>
        </Alert>
      ) : null}

      <RecordTable
        key={scanKey}
        columns={columns}
        data={records}
        toolbar={
          <>
            <SearchField
              className="min-w-48 flex-1"
              value={term}
              onChange={(event) => setTerm(event.target.value)}
              placeholder="Search key or value…"
            />

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

            <Select
              value={partition}
              items={partitionItems}
              onValueChange={(value) => setPartition(String(value))}
            >
              <SelectTrigger className="w-36">
                <SelectValue placeholder="Partition" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {partitionItems.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>

            <Select
              value={order}
              items={ORDER_ITEMS}
              onValueChange={(value) => setOrder(value as RecordOrder)}
            >
              <SelectTrigger className="w-28">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {ORDER_ITEMS.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>

            {showSchemaPicker ? (
              <SchemaPicker
                cluster={cluster}
                topic={topic.name}
                value={schemaId}
                onChange={setSchemaId}
              />
            ) : null}

            {obfuscated ? <ObfuscatedBadge /> : null}
          </>
        }
        getRowId={(record) => `${record.partition}-${record.offset}`}
        loading={isFetching && !isFetchingNextPage && records.length === 0}
        refreshing={isFetching && !isFetchingNextPage && records.length > 0}
        stale={isPlaceholderData}
        hasNextPage={Boolean(hasNextPage) && !isPlaceholderData}
        fetchNextPage={loadMore}
        isFetchingNextPage={isFetchingNextPage}
        isFetchNextPageError={isFetchNextPageError}
        onRowClick={setSelected}
        selectedKey={
          selectedRecord ? `${selectedRecord.partition}-${selectedRecord.offset}` : undefined
        }
        error={isError ? apiErrorMessage(error, "Failed to load records.") : undefined}
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
                  : term
                    ? "Nothing matched your search in the scanned offsets."
                    : "This topic has no records."}
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        }
      />

      <Sheet
        open={selectedRecord !== null}
        onOpenChange={(open) => {
          if (!open) {
            setSelected(null);
            setExpanded(false);
          }
        }}
      >
        <SheetContent
          side="right"
          className={cn(
            "w-full gap-0 data-[side=right]:w-full",
            expanded
              ? "data-[side=right]:sm:max-w-[min(90vw,56rem)]"
              : "data-[side=right]:sm:max-w-2xl",
          )}
        >
          {selectedRecord ? (
            <>
              <SheetHeader className="gap-1 border-b px-5 py-4 pr-12">
                <SheetTitle className="flex min-w-0 items-center gap-2 font-mono text-sm font-medium">
                  <span className="min-w-0 truncate">
                    <span className="text-muted-foreground">{topic.name}</span>
                    <span className="text-muted-foreground/60"> / </span>
                    {selectedRecord.partition}
                    <span className="text-muted-foreground/60"> @ </span>
                    {selectedRecord.offset}
                  </span>
                  {obfuscated ? <ObfuscatedBadge /> : null}
                </SheetTitle>
                <SheetDescription>
                  Produced {formatRelative(selectedRecord.timestamp)}
                </SheetDescription>
              </SheetHeader>

              <dl className="grid shrink-0 grid-cols-3 gap-x-4 gap-y-3 border-b px-5 py-4">
                <Meta label="Partition" value={selectedRecord.partition} />
                <Meta label="Offset" value={selectedRecord.offset} />
                <Meta label="Size" value={formatBytes(selectedRecord.sizeBytes)} />
                <Meta label="Timestamp" value={formatTimestamp(selectedRecord.timestamp)} />
                <Meta
                  label="Schema"
                  value={
                    selectedSchemaId != null ? (
                      <SchemaLink cluster={cluster} topic={topic.name} id={selectedSchemaId} />
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )
                  }
                />
                <Meta label="Compression" value={selectedRecord.compression.toLowerCase()} />
              </dl>

              <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-hidden px-5 py-4">
                <PayloadView
                  key={`key-${selectedRecord.partition}-${selectedRecord.offset}`}
                  label="Key"
                  source={selectedRecord.key ?? "null"}
                  copyLabel="Copy key"
                  showCopy={Boolean(selectedRecord.key)}
                />

                <PayloadView
                  key={`value-${selectedRecord.partition}-${selectedRecord.offset}`}
                  label="Value"
                  source={selectedRecord.value ?? "null"}
                  filename={`${topic.name}-${selectedRecord.partition}-${selectedRecord.offset}.json`}
                  copyLabel="Copy value"
                  showCopy={Boolean(selectedRecord.value)}
                  showDownload
                  showExpand
                  expanded={expanded}
                  onExpandedChange={setExpanded}
                  fill
                />

                <section className="shrink-0 space-y-2">
                  <h3 className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
                    Headers
                    <span className="numeric text-muted-foreground/60">
                      {selectedRecord.headers.length}
                    </span>
                  </h3>
                  {selectedRecord.headers.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No headers.</p>
                  ) : (
                    <dl className="grid max-h-40 grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-6 overflow-y-auto rounded-lg border bg-subtle px-3 py-2 font-mono text-sm">
                      {selectedRecord.headers.map((header) => (
                        <div key={header.key} className="contents">
                          <dt className="truncate py-1 text-muted-foreground">{header.key}</dt>
                          <dd className="py-1 break-all">{header.value}</dd>
                        </div>
                      ))}
                    </dl>
                  )}
                </section>
              </div>
            </>
          ) : null}
        </SheetContent>
      </Sheet>
    </div>
  );
}

function SchemaLink({ cluster, topic, id }: { cluster: string; topic: string; id: number }) {
  const { data } = useSubjectRows(cluster);
  // Several subjects can register the same schema; prefer the topic's own.
  const matches = data?.rows.filter((row) => row.id === id) ?? [];
  const subject = matches.find((row) => row.subject === `${topic}-value`) ?? matches[0];

  if (subject == null) {
    return id;
  }

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Link
            to="/cluster/$cluster/schemas"
            params={{ cluster }}
            search={{ subject: subject.subject, version: subject.latestVersion }}
            className="text-primary underline-offset-4 outline-none hover:underline focus-visible:underline"
          />
        }
      >
        {id}
      </TooltipTrigger>
      <TooltipContent>
        <span className="font-mono">{subject.subject}</span> · v{subject.latestVersion}
      </TooltipContent>
    </Tooltip>
  );
}

function Meta({
  label,
  value,
  className,
}: {
  label: string;
  value: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("min-w-0 space-y-0.5", className)}>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="numeric truncate text-sm">{value}</dd>
    </div>
  );
}
