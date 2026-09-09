import { useState } from "react";
import { ClockIcon, SearchIcon } from "lucide-react";

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
import { DataTable, type Column } from "@/components/data-table";
import { PayloadView } from "@/components/payload-view";
import { Pill } from "@/components/status";
import { useRecords } from "@/lib/api/queries";
import { formatBytes, formatRelative, formatTimestamp, fromDatetimeLocalValue } from "@/lib/format";
import type { RecordOrder, Topic, TopicRecord } from "@/lib/api/types";
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

export function RecordBrowser({ cluster, topic }: { cluster: string; topic: Topic }) {
  const [partition, setPartition] = useState<string>("all");
  const [term, setTerm] = useState("");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [limit, setLimit] = useState("50");
  const [order, setOrder] = useState<RecordOrder>("NEWEST");
  const [page, setPage] = useState(0);
  const [selected, setSelected] = useState<TopicRecord | null>(null);
  const [expanded, setExpanded] = useState(false);

  const timestampFrom = fromDatetimeLocalValue(from);
  const timestampTo = fromDatetimeLocalValue(to);
  const { data, isFetching } = useRecords({
    cluster,
    topic: topic.name,
    partition: partition === "all" ? null : Number(partition),
    search: term,
    timestampFrom,
    timestampTo,
    limit: Number(limit),
    order,
    page,
  });
  const records = data?.records ?? [];
  const hasMore = data?.hasMore ?? false;
  const partitionItems = [
    { value: "all", label: "All partitions" },
    ...topic.partitions.map((part) => ({
      value: String(part.id),
      label: `Partition ${part.id}`,
    })),
  ];

  const columns: Array<Column<TopicRecord>> = [
    {
      id: "partition",
      header: "Part",
      align: "right",
      sortValue: (record) => record.partition,
      cell: (record) => <span className="numeric font-mono">{record.partition}</span>,
      headerClassName: "w-16",
    },
    {
      id: "offset",
      header: "Offset",
      align: "right",
      sortValue: (record) => record.offset,
      cell: (record) => <span className="numeric font-mono">{record.offset}</span>,
      headerClassName: "w-28",
    },
    {
      id: "key",
      header: "Key",
      sortValue: (record) => record.key ?? "",
      cell: (record) => (
        <span className="block max-w-48 truncate font-mono text-sm text-brand">
          {record.key ?? "null"}
        </span>
      ),
    },
    {
      id: "value",
      header: "Value",
      cell: (record) => (
        <span className="block max-w-md truncate font-mono text-sm text-muted-foreground lg:max-w-2xl">
          {preview(record.value)}
        </span>
      ),
    },
    {
      id: "size",
      header: "Size",
      align: "right",
      sortValue: (record) => record.sizeBytes,
      cell: (record) => <span className="numeric">{formatBytes(record.sizeBytes)}</span>,
    },
    {
      id: "timestamp",
      header: "Timestamp",
      align: "right",
      sortValue: (record) => record.timestamp,
      cell: (record) => (
        <Tooltip>
          <TooltipTrigger render={<span className="numeric cursor-default whitespace-nowrap" />}>
            {formatTimestamp(record.timestamp)}
          </TooltipTrigger>
          <TooltipContent>{formatRelative(record.timestamp)}</TooltipContent>
        </Tooltip>
      ),
    },
  ];

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <InputGroup className="w-full max-w-sm">
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
          <InputGroupInput
            value={term}
            onChange={(event) => {
              setTerm(event.target.value);
              setPage(0);
            }}
            placeholder="Search key or value…"
          />
        </InputGroup>

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
              setPage(0);
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
              setPage(0);
            }}
            aria-label="To timestamp"
          />
        </InputGroup>

        <Select
          value={partition}
          items={partitionItems}
          onValueChange={(value) => {
            setPartition(String(value));
            setPage(0);
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
            setPage(0);
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
            setPage(0);
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

        <span className="ml-auto text-sm text-muted-foreground">
          {isFetching ? "polling…" : `${records.length} records`}
        </span>
      </div>

      <DataTable
        columns={columns}
        rows={records}
        rowKey={(record) => `${record.partition}-${record.offset}`}
        loading={isFetching && records.length === 0}
        pageSize={Number(limit)}
        page={page}
        hasMore={hasMore}
        onPageChange={setPage}
        onRowClick={setSelected}
        selectedKey={selected ? `${selected.partition}-${selected.offset}` : undefined}
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
        open={selected !== null}
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
          {selected ? (
            <>
              <SheetHeader className="border-b">
                <SheetTitle className="font-mono text-sm">
                  {topic.name}[{selected.partition}]@{selected.offset}
                </SheetTitle>
                <SheetDescription>
                  {formatTimestamp(selected.timestamp)} · {formatBytes(selected.sizeBytes)} ·{" "}
                  {selected.compression.toLowerCase()}
                </SheetDescription>
              </SheetHeader>

              <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-hidden p-4">
                <PayloadView
                  key={`key-${selected.partition}-${selected.offset}`}
                  label="Key"
                  source={selected.key ?? "null"}
                  copyLabel="Copy key"
                  showCopy={Boolean(selected.key)}
                />

                <PayloadView
                  key={`value-${selected.partition}-${selected.offset}`}
                  label="Value"
                  source={selected.value ?? "null"}
                  filename={`${topic.name}-${selected.partition}-${selected.offset}.json`}
                  copyLabel="Copy value"
                  showCopy={Boolean(selected.value)}
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
                  {selected.headers.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No headers.</p>
                  ) : (
                    <div className="divide-y overflow-hidden rounded-lg border">
                      {selected.headers.map((header) => (
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
                    <Meta label="Partition" value={String(selected.partition)} />
                    <Meta label="Offset" value={String(selected.offset)} />
                    <Meta label="Timestamp" value={formatTimestamp(selected.timestamp)} />
                    <Meta label="Age" value={formatRelative(selected.timestamp)} />
                    <Meta label="Size" value={formatBytes(selected.sizeBytes)} />
                    <Meta
                      label="Compression"
                      value={<Pill>{selected.compression.toLowerCase()}</Pill>}
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
