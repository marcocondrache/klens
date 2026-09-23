import { useCallback, useMemo, useState } from "react";
import {
  BrushCleaningIcon,
  ClockIcon,
  EyeOffIcon,
  PauseIcon,
  PlayIcon,
  RadioIcon,
  TriangleAlertIcon,
} from "lucide-react";
import { createColumnHelper } from "@tanstack/react-table";

import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
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
import { Item, ItemContent, ItemGroup, ItemTitle } from "@/components/ui/item";
import { Toggle } from "@/components/ui/toggle";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { type DataTableFeatures } from "@/components/data-table/features";
import { RecordTable } from "@/components/records/record-table";
import { PayloadView } from "@/components/payload-view";
import { SchemaPicker } from "@/components/schema-picker";
import { SearchField } from "@/components/search-field";
import { Pill, StatusDot } from "@/components/status";
import { useRecords, type RecordsFilter } from "@/lib/api/live";
import { TAIL_BUFFER, recordId, useTail, type TailFilter, type TailStatus } from "@/lib/api/tail";
import { useAccess } from "@/hooks/use-access";
import { apiErrorMessage } from "@/lib/api/client";
import {
  formatBytes,
  formatNumber,
  formatRelative,
  formatTimestamp,
  fromDatetimeLocalValue,
} from "@/lib/format";
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

const TAIL_STATUS_LABEL: Record<TailStatus, string> = {
  idle: "Paused",
  connecting: "Connecting…",
  live: "Live",
  reconnecting: "Reconnecting…",
  error: "Stopped",
};

function TailStatusPill({ status, paused }: { status: TailStatus; paused: boolean }) {
  const tone =
    paused || status === "idle"
      ? "idle"
      : status === "live"
        ? "ok"
        : status === "error"
          ? "error"
          : "warn";

  return (
    <Pill tone={tone}>
      <StatusDot tone={tone} pulse={!paused && status === "live"} />
      {paused ? "Paused" : TAIL_STATUS_LABEL[status]}
    </Pill>
  );
}

function SkippedBadge({ skipped }: { skipped: number }) {
  return (
    <Tooltip>
      <TooltipTrigger render={<Pill tone="warn" className="cursor-default" />}>
        {formatNumber(skipped)} skipped
      </TooltipTrigger>
      <TooltipContent className="block max-w-80 py-2 leading-relaxed">
        This topic produces faster than a live tail shows. The tail samples it: each update keeps
        the newest records and passes over the rest.
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
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Offset" className="justify-end" />
    ),
    meta: { align: "right", headerClassName: "w-28" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Key" />,
    cell: ({ row }) => (
      <span className="block max-w-48 truncate font-mono text-sm text-brand">
        {row.original.key ?? "null"}
      </span>
    ),
  }),
  columnHelper.accessor((record) => record.value ?? "", {
    id: "value",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Value" />,
    cell: ({ row }) => (
      <span className="block max-w-md truncate font-mono text-sm text-muted-foreground lg:max-w-2xl">
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
    cell: ({ getValue }) => <span className="numeric">{formatBytes(getValue())}</span>,
  }),
  columnHelper.accessor("timestamp", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Timestamp" className="justify-end" />
    ),
    meta: { align: "right" },
    sortFn: "datetime",
    cell: ({ getValue }) => (
      <Tooltip>
        <TooltipTrigger render={<span className="numeric cursor-default whitespace-nowrap" />}>
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
  const [live, setLive] = useState(false);
  const [paused, setPaused] = useState(false);

  const { can } = useAccess();
  const canRecords = can(cluster, "RECORDS");

  const query = useMemo<RecordsFilter>(() => {
    const needle = term.trim();

    return {
      topic: topic.name,
      partition: partition === "all" ? null : Number(partition),
      order,
      from: fromDatetimeLocalValue(from),
      to: fromDatetimeLocalValue(to),
      filter: needle ? { contains: needle } : null,
      schemaId,
    };
  }, [topic.name, partition, order, from, to, term, schemaId]);

  const tailFilter = useMemo<TailFilter>(() => {
    const needle = term.trim();

    return {
      topic: topic.name,
      partition: partition === "all" ? null : Number(partition),
      contains: needle || null,
      schemaId,
    };
  }, [topic.name, partition, term, schemaId]);

  const tail = useTail(cluster, tailFilter, canRecords && live && !paused);

  const {
    data,
    isFetching,
    isFetchingNextPage,
    isFetchNextPageError,
    isError,
    error,
    fetchNextPage,
    hasNextPage,
  } = useRecords(cluster, query, canRecords && !live);
  const loadMore = useCallback(() => {
    void fetchNextPage();
  }, [fetchNextPage]);

  const pageRecords = useMemo(() => {
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
  const records = live ? tail.records : pageRecords;
  const lastPage = data?.pages[data.pages.length - 1];
  const scanKey = live ? JSON.stringify(["tail", tailFilter]) : JSON.stringify(query);
  const obfuscated = live
    ? tail.obfuscated
    : (data?.pages.some((page) => page.obfuscated) ?? false);
  const tailConnecting = tail.status === "connecting" || tail.status === "reconnecting";
  const showSchemaPicker =
    schemaId != null || records.some((record) => record.value != null && record.schemaId == null);
  const selectedRecord =
    selected == null
      ? null
      : (records.find(
          (record) => record.partition === selected.partition && record.offset === selected.offset,
        ) ?? selected);

  const partitionItems = [
    { value: "all", label: "All partitions" },
    ...topic.partitions.map((part) => ({
      value: String(part.id),
      label: `Partition ${part.id}`,
    })),
  ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {live && tail.status === "error" ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Live tail stopped</AlertTitle>
          <AlertDescription>{apiErrorMessage(tail.error, "The live tail ended.")}</AlertDescription>
          <AlertAction>
            <Button variant="outline" size="sm" onClick={tail.retry}>
              Retry
            </Button>
          </AlertAction>
        </Alert>
      ) : null}

      {!live && lastPage && !lastPage.complete ? (
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
              value={term}
              onChange={(event) => setTerm(event.target.value)}
              placeholder="Search key or value…"
            />

            {live ? null : (
              <>
                <InputGroup className="w-auto min-w-[13.5rem]">
                  <InputGroupAddon>
                    <ClockIcon />
                  </InputGroupAddon>
                  <InputGroupInput
                    type="datetime-local"
                    value={from}
                    max={to || undefined}
                    onChange={(event) => setFrom(event.target.value)}
                    aria-label="From timestamp"
                  />
                </InputGroup>

                <InputGroup className="w-auto min-w-[13.5rem]">
                  <InputGroupAddon>
                    <span className="text-sm">to</span>
                  </InputGroupAddon>
                  <InputGroupInput
                    type="datetime-local"
                    value={to}
                    min={from || undefined}
                    onChange={(event) => setTo(event.target.value)}
                    aria-label="To timestamp"
                  />
                </InputGroup>
              </>
            )}

            <Select
              value={partition}
              items={partitionItems}
              onValueChange={(value) => setPartition(String(value))}
            >
              <SelectTrigger className="w-40">
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

            {live ? null : (
              <Select
                value={order}
                items={ORDER_ITEMS}
                onValueChange={(value) => setOrder(value as RecordOrder)}
              >
                <SelectTrigger className="w-36">
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
            )}

            {showSchemaPicker ? (
              <SchemaPicker
                cluster={cluster}
                topic={topic.name}
                value={schemaId}
                onChange={setSchemaId}
              />
            ) : null}

            {obfuscated ? <ObfuscatedBadge /> : null}

            <div className="ml-auto flex items-center gap-2">
              {live ? (
                <>
                  {tail.skipped > 0 ? <SkippedBadge skipped={tail.skipped} /> : null}
                  <TailStatusPill status={tail.status} paused={paused} />
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <Button
                          variant="outline"
                          size="icon"
                          aria-label={paused ? "Resume live tail" : "Pause live tail"}
                          onClick={() => setPaused((value) => !value)}
                        />
                      }
                    >
                      {paused ? <PlayIcon /> : <PauseIcon />}
                    </TooltipTrigger>
                    <TooltipContent>{paused ? "Resume" : "Pause"}</TooltipContent>
                  </Tooltip>
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <Button
                          variant="outline"
                          size="icon"
                          aria-label="Clear records"
                          disabled={tail.records.length === 0 && tail.skipped === 0}
                          onClick={tail.clear}
                        />
                      }
                    >
                      <BrushCleaningIcon />
                    </TooltipTrigger>
                    <TooltipContent>Clear</TooltipContent>
                  </Tooltip>
                </>
              ) : null}
              <Toggle
                variant="outline"
                pressed={live}
                onPressedChange={(pressed) => {
                  setLive(pressed);
                  setPaused(false);
                }}
                aria-label="Live tail"
              >
                <RadioIcon />
                Live
              </Toggle>
            </div>
          </>
        }
        getRowId={recordId}
        loading={!live && isFetching && !isFetchingNextPage && records.length === 0}
        refreshing={live ? tailConnecting : isFetching && !isFetchingNextPage && records.length > 0}
        hasNextPage={!live && Boolean(hasNextPage)}
        fetchNextPage={loadMore}
        isFetchingNextPage={!live && isFetchingNextPage}
        isFetchNextPageError={!live && isFetchNextPageError}
        onRowClick={setSelected}
        selectedKey={selectedRecord ? recordId(selectedRecord) : undefined}
        error={!live && isError ? apiErrorMessage(error, "Failed to load records.") : undefined}
        emptyState={
          live ? (
            <Empty className="py-10">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <RadioIcon />
                </EmptyMedia>
                <EmptyTitle>
                  {paused
                    ? "Live tail paused"
                    : tail.status === "error"
                      ? "Live tail stopped"
                      : "Waiting for records"}
                </EmptyTitle>
                <EmptyDescription>
                  {paused
                    ? "Resume to follow the topic from its current end."
                    : tail.status === "error"
                      ? "Retry to follow the topic again."
                      : `Following ${topic.name} from its current end. New records show here as they arrive, newest first. The last ${formatNumber(TAIL_BUFFER)} stay on screen.`}
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
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
          )
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
              <SheetHeader className="border-b">
                <SheetTitle className="flex items-center gap-2 font-mono text-sm">
                  {topic.name}[{selectedRecord.partition}]@{selectedRecord.offset}
                  {obfuscated ? <ObfuscatedBadge /> : null}
                </SheetTitle>
                <SheetDescription>
                  {formatTimestamp(selectedRecord.timestamp)} ·{" "}
                  {formatBytes(selectedRecord.sizeBytes)} ·{" "}
                  {selectedRecord.compression.toLowerCase()}
                </SheetDescription>
              </SheetHeader>

              <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-hidden p-4">
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
                  <h3 className="text-sm font-medium text-muted-foreground">Headers</h3>
                  {selectedRecord.headers.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No headers.</p>
                  ) : (
                    <ItemGroup className="gap-0 overflow-hidden rounded-lg border">
                      {selectedRecord.headers.map((header) => (
                        <Item
                          key={header.key}
                          size="sm"
                          className="rounded-none border-b last:border-b-0"
                        >
                          <ItemContent className="flex-row items-start justify-between gap-3">
                            <ItemTitle className="font-mono font-normal text-brand">
                              {header.key}
                            </ItemTitle>
                            <span className="max-w-[60%] font-mono text-sm break-all">
                              {header.value}
                            </span>
                          </ItemContent>
                        </Item>
                      ))}
                    </ItemGroup>
                  )}
                </section>

                <section className="shrink-0 space-y-2">
                  <h3 className="text-sm font-medium text-muted-foreground">Metadata</h3>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <Meta label="Partition" value={String(selectedRecord.partition)} />
                    <Meta label="Offset" value={String(selectedRecord.offset)} />
                    <Meta label="Timestamp" value={formatTimestamp(selectedRecord.timestamp)} />
                    <Meta label="Age" value={formatRelative(selectedRecord.timestamp)} />
                    <Meta label="Size" value={formatBytes(selectedRecord.sizeBytes)} />
                    <Meta
                      label="Compression"
                      value={<Pill>{selectedRecord.compression.toLowerCase()}</Pill>}
                    />
                  </div>
                </section>
              </div>
            </>
          ) : null}
        </SheetContent>
      </Sheet>
    </div>
  );
}

function Meta({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <Item variant="outline" size="sm">
      <ItemContent>
        <ItemTitle className="font-normal text-muted-foreground">{label}</ItemTitle>
        <div className="numeric font-mono text-sm">{value}</div>
      </ItemContent>
    </Item>
  );
}
