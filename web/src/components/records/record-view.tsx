import { useState, type ReactNode } from "react";
import { EyeOffIcon } from "lucide-react";
import { createColumnHelper } from "@tanstack/react-table";

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
import { formatBytes, formatRelative, formatTimestamp } from "@/lib/format";
import { recordId } from "@/lib/records";
import type { KafkaRecord, TopicDetail } from "@/lib/api/types";
import { cn } from "@/lib/utils";

export type RecordFilter = {
  term: string;
  partition: number | null;
  schemaId: number | null;
};

export const EMPTY_FILTER: RecordFilter = { term: "", partition: null, schemaId: null };

export type RecordSource = {
  records: KafkaRecord[];
  scope: string;
  obfuscated: boolean;
  loading: boolean;
  refreshing: boolean;
  error?: string;
  pages?: {
    hasNextPage: boolean;
    fetchNextPage: () => void;
    isFetchingNextPage: boolean;
    isFetchNextPageError: boolean;
  };
};

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

type RecordViewProps = {
  cluster: string;
  topic: TopicDetail;
  source: RecordSource;
  filter: RecordFilter;
  onFilterChange: (filter: RecordFilter) => void;
  controls?: ReactNode;
  actions?: ReactNode;
  notice?: ReactNode;
  emptyState: ReactNode;
};

export function RecordView({
  cluster,
  topic,
  source,
  filter,
  onFilterChange,
  controls,
  actions,
  notice,
  emptyState,
}: RecordViewProps) {
  const [selected, setSelected] = useState<KafkaRecord | null>(null);
  const [expanded, setExpanded] = useState(false);

  const { records, obfuscated } = source;
  const showSchemaPicker =
    filter.schemaId != null ||
    records.some((record) => record.value != null && record.schemaId == null);
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
      {notice}

      <RecordTable
        key={source.scope}
        columns={columns}
        data={records}
        toolbar={
          <>
            <SearchField
              className="min-w-48 flex-1"
              value={filter.term}
              onChange={(event) => onFilterChange({ ...filter, term: event.target.value })}
              placeholder="Search key or value…"
            />

            <Select
              value={filter.partition == null ? "all" : String(filter.partition)}
              items={partitionItems}
              onValueChange={(value) =>
                onFilterChange({
                  ...filter,
                  partition: value === "all" ? null : Number(value),
                })
              }
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

            {controls}

            {showSchemaPicker ? (
              <SchemaPicker
                cluster={cluster}
                topic={topic.name}
                value={filter.schemaId}
                onChange={(schemaId) => onFilterChange({ ...filter, schemaId })}
              />
            ) : null}

            {obfuscated ? <ObfuscatedBadge /> : null}

            {actions ? <div className="ml-auto flex items-center gap-2">{actions}</div> : null}
          </>
        }
        getRowId={recordId}
        loading={source.loading}
        refreshing={source.refreshing}
        hasNextPage={source.pages?.hasNextPage}
        fetchNextPage={source.pages?.fetchNextPage}
        isFetchingNextPage={source.pages?.isFetchingNextPage}
        isFetchNextPageError={source.pages?.isFetchNextPageError}
        onRowClick={setSelected}
        selectedKey={selectedRecord ? recordId(selectedRecord) : undefined}
        error={source.error}
        emptyState={emptyState}
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
                <Meta
                  label="Timestamp"
                  value={formatTimestamp(selectedRecord.timestamp)}
                  className="col-span-2"
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
