import { useMemo, useState } from "react";
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
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PayloadView } from "@/components/payload-view";
import { SchemaPicker } from "@/components/schema-picker";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useSubjectRows } from "@/lib/api/catalog";
import { useRecords, type RecordsFilter } from "@/lib/api/live";
import { useAccess } from "@/hooks/use-access";
import { queryErrorMessage } from "@/lib/query-error";
import { formatBytes, formatRelative, formatTimestamp, fromDatetimeLocalValue } from "@/lib/format";
import type { KafkaRecord, RecordOrder, TopicDetail } from "@/lib/api/types";
import { cn } from "@/lib/utils";

const LIMITS = ["25", "50", "100"] as const;

const ORDER_ITEMS = [
  { value: "NEWEST", label: "Newest first" },
  { value: "OLDEST", label: "Oldest first" },
] as const;

const LIMIT_ITEMS = LIMITS.map((value) => ({
  value,
  label: `${value} rows`,
}));

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
        A rule on this cluster hides parts of this topic. Protected fields render as *** or as kx:
        tokens — equal values share a token, so records still correlate, and a masked number renders
        as text. A value the registry could not decode is masked whole. Searches match this view,
        never the value behind it.
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
    meta: { align: "right", headerClassName: "w-16", label: "Part" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Offset" className="justify-end" />
    ),
    meta: { align: "right", headerClassName: "w-28", label: "Offset" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Key" />,
    meta: { label: "Key" },
    cell: ({ row }) => (
      <span className="block max-w-48 truncate font-mono text-sm text-brand">
        {row.original.key ?? "null"}
      </span>
    ),
  }),
  columnHelper.accessor((record) => record.value ?? "", {
    id: "value",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Value" />,
    meta: { label: "Value" },
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
  const [limit, setLimit] = useState("50");
  const [order, setOrder] = useState<RecordOrder>("NEWEST");
  const [cursor, setCursor] = useState<string | null>(null);
  const [pageIndex, setPageIndex] = useState(0);
  const [selected, setSelected] = useState<KafkaRecord | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [schemaId, setSchemaId] = useState<number | null>(null);

  const { can } = useAccess();
  const { data: subjects } = useSubjectRows(cluster);

  const query = useMemo<RecordsFilter>(() => {
    const needle = term.trim();

    return {
      topic: topic.name,
      partition: partition === "all" ? null : Number(partition),
      order,
      from: fromDatetimeLocalValue(from),
      to: fromDatetimeLocalValue(to),
      limit: Number(limit),
      filter: needle ? { contains: needle, cel: null } : null,
      schemaId,
    };
  }, [topic.name, partition, order, from, to, limit, term, schemaId]);

  const { data, isFetching, isPlaceholderData, isError, error } = useRecords(
    cluster,
    query,
    cursor,
    can(cluster, "RECORDS"),
  );

  const records = data?.records ?? [];
  const showSchemaPicker =
    schemaId != null || records.some((record) => record.value != null && record.schemaId == null);
  const selectedRecord =
    selected == null
      ? null
      : (records.find(
          (record) => record.partition === selected.partition && record.offset === selected.offset,
        ) ?? selected);

  function rewind() {
    setCursor(null);
    setPageIndex(0);
  }

  function step(next: string | null, delta: number) {
    if (next == null) return;
    setCursor(next);
    setPageIndex((current) => Math.max(0, current + delta));
  }

  const partitionItems = [
    { value: "all", label: "All partitions" },
    ...topic.partitions.map((part) => ({
      value: String(part.id),
      label: `Partition ${part.id}`,
    })),
  ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {data && !data.complete ? (
        <Alert>
          <TriangleAlertIcon />
          <AlertTitle>Partial page</AlertTitle>
          <AlertDescription>
            The scan hit its deadline with offsets still unread. These records match, but the page
            is not everything the query matched — continue to keep scanning.
          </AlertDescription>
        </Alert>
      ) : null}

      <DataTable
        columns={columns}
        data={records}
        toolbar={
          <>
            <SearchField
              value={term}
              onChange={(event) => {
                setTerm(event.target.value);
                rewind();
              }}
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
                onChange={(event) => {
                  setFrom(event.target.value);
                  rewind();
                }}
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
                onChange={(event) => {
                  setTo(event.target.value);
                  rewind();
                }}
                aria-label="To timestamp"
              />
            </InputGroup>

            <Select
              value={partition}
              items={partitionItems}
              onValueChange={(value) => {
                setPartition(String(value));
                rewind();
              }}
            >
              <SelectTrigger size="sm" className="w-40">
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
              onValueChange={(value) => {
                setOrder(value as RecordOrder);
                rewind();
              }}
            >
              <SelectTrigger size="sm" className="w-36">
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

            <Select
              value={limit}
              items={LIMIT_ITEMS}
              onValueChange={(value) => {
                setLimit(String(value));
                rewind();
              }}
            >
              <SelectTrigger size="sm" className="w-28">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {LIMIT_ITEMS.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>

            {showSchemaPicker ? (
              <SchemaPicker
                subjects={subjects?.rows ?? []}
                topic={topic.name}
                value={schemaId}
                onChange={(id) => {
                  setSchemaId(id);
                  rewind();
                }}
              />
            ) : null}

            {data?.obfuscated ? <ObfuscatedBadge /> : null}
          </>
        }
        getRowId={(record) => `${record.partition}-${record.offset}`}
        loading={isFetching && records.length === 0}
        refreshing={isFetching && records.length > 0}
        pageSize={Number(limit)}
        pageIndex={pageIndex}
        hasMore={data?.nextCursor != null}
        loadingMore={isFetching && isPlaceholderData}
        canPreviousPage={data?.prevCursor != null || pageIndex > 0}
        onPreviousPage={() => {
          if (data?.prevCursor == null) {
            rewind();
            return;
          }
          step(data.prevCursor, -1);
        }}
        onNextPage={() => step(data?.nextCursor ?? null, 1)}
        onRowClick={setSelected}
        selectedKey={
          selectedRecord ? `${selectedRecord.partition}-${selectedRecord.offset}` : undefined
        }
        fill
        error={queryErrorMessage(isError, error, "Failed to load records.")}
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
                    : "This topic has no records in the selected range."}
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
                  {data?.obfuscated ? <ObfuscatedBadge /> : null}
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
                    <div className="divide-y overflow-hidden border">
                      {selectedRecord.headers.map((header) => (
                        <div
                          key={header.key}
                          className="flex items-start justify-between gap-3 px-3 py-2"
                        >
                          <span className="font-mono text-sm text-brand">{header.key}</span>
                          <span className="max-w-[60%] font-mono text-sm break-all">
                            {header.value}
                          </span>
                        </div>
                      ))}
                    </div>
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
    <div className="border bg-muted/20 px-3 py-2">
      <p className="text-sm text-muted-foreground">{label}</p>
      <p className="numeric mt-0.5 font-mono text-sm">{value}</p>
    </div>
  );
}
