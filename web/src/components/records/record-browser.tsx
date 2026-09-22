import { useCallback, useMemo, useState } from "react";
import { ClockIcon, EyeOffIcon, TriangleAlertIcon } from "lucide-react";
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
import { Item, ItemContent, ItemGroup, ItemTitle } from "@/components/ui/item";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { type DataTableFeatures } from "@/components/data-table/features";
import { RecordTable } from "@/components/records/record-table";
import { PayloadView } from "@/components/payload-view";
import { SchemaPicker } from "@/components/schema-picker";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useRecords, type RecordsFilter } from "@/lib/api/live";
import { useAccess } from "@/hooks/use-access";
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
    meta: { align: "right", label: "Part" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Offset" className="justify-end" />
    ),
    meta: { align: "right", label: "Offset" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Key" />,
    meta: { label: "Key" },
    cell: ({ row }) => (
      <span className="font-mono text-sm text-brand">{row.original.key ?? "null"}</span>
    ),
  }),
  columnHelper.accessor((record) => record.value ?? "", {
    id: "value",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Value" />,
    meta: { label: "Value" },
    cell: ({ row }) => (
      <span className="font-mono text-sm text-muted-foreground">{preview(row.original.value)}</span>
    ),
  }),
  columnHelper.accessor("sizeBytes", {
    id: "size",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Size" className="justify-end" />
    ),
    meta: { align: "right", label: "Size" },
    cell: ({ getValue }) => <span className="numeric">{formatBytes(getValue())}</span>,
  }),
  columnHelper.accessor("timestamp", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Timestamp" className="justify-end" />
    ),
    meta: { align: "right", label: "Timestamp" },
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

  const { can } = useAccess();

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

  const {
    data,
    isFetching,
    isFetchingNextPage,
    isFetchNextPageError,
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

  const partitionItems = [
    { value: "all", label: "All partitions" },
    ...topic.partitions.map((part) => ({
      value: String(part.id),
      label: `Partition ${part.id}`,
    })),
  ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {lastPage && !lastPage.complete ? (
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
        hasNextPage={Boolean(hasNextPage)}
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
