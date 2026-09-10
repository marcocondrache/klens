import { useState } from "react";
import { ClockIcon } from "lucide-react";

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
import { DataTable } from "@/components/data-table";
import { PayloadView } from "@/components/payload-view";
import { SchemaPicker } from "@/components/schema-picker";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useRecords, useSchemaSubjects } from "@/lib/api/queries";
import { formatBytes, formatRelative, formatTimestamp, fromDatetimeLocalValue } from "@/lib/format";
import type { RecordOrder, Topic, TopicRecord } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";
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

/** Compile the search box into the backend CEL `filter` field. */
function containsFilter(term: string): string | null {
  const trimmed = term.trim();
  if (!trimmed) return null;
  const needle = JSON.stringify(trimmed);
  return `keyText.lowerAscii().contains(${needle}) || valueText.lowerAscii().contains(${needle})`;
}

const columnHelper = createAppColumnHelper<TopicRecord>();

const columns = columnHelper.columns([
  columnHelper.accessor("partition", {
    header: "Part",
    meta: { align: "right", headerClassName: "w-16" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    header: "Offset",
    meta: { align: "right", headerClassName: "w-28" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    header: "Key",
    cell: ({ row }) => (
      <span className="block max-w-48 truncate font-mono text-sm text-brand">
        {row.original.key ?? "null"}
      </span>
    ),
  }),
  columnHelper.display({
    id: "value",
    header: "Value",
    cell: ({ row }) => (
      <span className="block max-w-md truncate font-mono text-sm text-muted-foreground lg:max-w-2xl">
        {preview(row.original.value)}
      </span>
    ),
  }),
  columnHelper.accessor("sizeBytes", {
    id: "size",
    header: "Size",
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric">{formatBytes(getValue())}</span>,
  }),
  columnHelper.accessor("timestamp", {
    header: "Timestamp",
    meta: { align: "right" },
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

export function RecordBrowser({ cluster, topic }: { cluster: string; topic: Topic }) {
  const [partition, setPartition] = useState<string>("all");
  const [term, setTerm] = useState("");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [limit, setLimit] = useState("50");
  const [order, setOrder] = useState<RecordOrder>("NEWEST");
  const [pageIndex, setPageIndex] = useState(0);
  const [selected, setSelected] = useState<TopicRecord | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [schemaId, setSchemaId] = useState<number | null>(null);

  const timestampFrom = fromDatetimeLocalValue(from);
  const timestampTo = fromDatetimeLocalValue(to);
  const filter = containsFilter(term);
  const { data: subjects = [] } = useSchemaSubjects(cluster);
  const { data, isFetching, hasNextPage, fetchNextPage, isFetchingNextPage } = useRecords({
    cluster,
    topic: topic.name,
    partition: partition === "all" ? null : Number(partition),
    filter,
    timestampFrom,
    timestampTo,
    limit: Number(limit),
    order,
    schemaId,
  });
  const pages = data?.pages ?? [];
  const currentPage = pages[pageIndex];
  const records = currentPage?.records ?? [];
  const hasCachedNextPage = Boolean(pages[pageIndex + 1]);
  const hasMore = hasCachedNextPage || (pageIndex === pages.length - 1 && Boolean(hasNextPage));
  const showSchemaPicker =
    schemaId != null ||
    pages.some((page) =>
      page.records.some((record) => record.value != null && record.schemaId == null),
    );
  const selectedRecord =
    selected == null
      ? null
      : (pages
          .flatMap((page) => page.records)
          .find(
            (record) =>
              record.partition === selected.partition && record.offset === selected.offset,
          ) ?? selected);

  function resetPages() {
    setPageIndex(0);
  }

  function selectSchema(id: number | null) {
    setSchemaId(id);
    resetPages();
  }

  async function goToNextPage() {
    const nextPageIndex = pageIndex + 1;
    if (!pages[nextPageIndex]) {
      const result = await fetchNextPage();
      if (!result.data?.pages[nextPageIndex]) return;
    }
    setPageIndex(nextPageIndex);
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
      <div className="flex shrink-0 flex-wrap items-center gap-3">
        <SearchField
          value={term}
          onChange={(event) => {
            setTerm(event.target.value);
            resetPages();
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
              resetPages();
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
              resetPages();
            }}
            aria-label="To timestamp"
          />
        </InputGroup>

        <Select
          value={partition}
          items={partitionItems}
          onValueChange={(value) => {
            setPartition(String(value));
            resetPages();
          }}
        >
          <SelectTrigger size="sm" className="w-40">
            <SelectValue placeholder="Partition" />
          </SelectTrigger>
          <SelectContent>
            {partitionItems.map((item) => (
              <SelectItem key={item.value} value={item.value}>
                {item.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={order}
          items={ORDER_ITEMS}
          onValueChange={(value) => {
            setOrder(value as RecordOrder);
            resetPages();
          }}
        >
          <SelectTrigger size="sm" className="w-36">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {ORDER_ITEMS.map((item) => (
              <SelectItem key={item.value} value={item.value}>
                {item.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={limit}
          items={LIMIT_ITEMS}
          onValueChange={(value) => {
            setLimit(String(value));
            resetPages();
          }}
        >
          <SelectTrigger size="sm" className="w-28">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LIMIT_ITEMS.map((item) => (
              <SelectItem key={item.value} value={item.value}>
                {item.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        {showSchemaPicker ? (
          <SchemaPicker
            subjects={subjects}
            topic={topic.name}
            value={schemaId}
            onChange={selectSchema}
          />
        ) : null}

        <span className="ml-auto text-sm text-muted-foreground">{records.length} records</span>
      </div>

      <DataTable
        columns={columns}
        data={records}
        getRowId={(record) => `${record.partition}-${record.offset}`}
        loading={isFetching && records.length === 0 && !isFetchingNextPage}
        refreshing={(isFetching && records.length > 0) || isFetchingNextPage}
        pageSize={Number(limit)}
        pageIndex={pageIndex}
        hasMore={hasMore && !isFetchingNextPage}
        canPreviousPage={pageIndex > 0}
        onPreviousPage={() => setPageIndex((current) => Math.max(0, current - 1))}
        onNextPage={() => {
          void goToNextPage();
        }}
        onRowClick={setSelected}
        selectedKey={
          selectedRecord ? `${selectedRecord.partition}-${selectedRecord.offset}` : undefined
        }
        fill
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
                    ? "Nothing matched your search in the scanned offset window."
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
                <SheetTitle className="font-mono text-sm">
                  {topic.name}[{selectedRecord.partition}]@{selectedRecord.offset}
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
                  <h3 className="text-sm font-medium tracking-wide text-muted-foreground">
                    Headers
                  </h3>
                  {selectedRecord.headers.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No headers.</p>
                  ) : (
                    <div className="divide-y overflow-hidden rounded-lg border">
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
                  <h3 className="text-sm font-medium tracking-wide text-muted-foreground">
                    Metadata
                  </h3>
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
    <div className="rounded-lg border bg-muted/20 px-3 py-2">
      <p className="text-sm tracking-wide text-muted-foreground">{label}</p>
      <p className="numeric mt-0.5 font-mono text-sm">{value}</p>
    </div>
  );
}
