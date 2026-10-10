import { useRef, useState, type ReactNode } from "react";
import { CopyPlusIcon, EyeOffIcon, ListXIcon, Rows3Icon, SearchXIcon } from "lucide-react";
import { Link } from "@tanstack/react-router";
import { createColumnHelper, type ColumnSizingState } from "@tanstack/react-table";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { Spinner } from "@/components/ui/spinner";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { type DataTableFeatures } from "@/components/data-table/features";
import { FilterBar, type CustomFilter } from "@/components/data-table/filter-bar";
import {
  selectedOptions,
  type FilterField,
  type FilterRule,
} from "@/components/data-table/filters";
import { IconButton } from "@/components/icon-button";
import { PayloadView } from "@/components/payload-view";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { versionWithId } from "@/features/schemas/subject-search";
import { useAccess } from "@/hooks/use-access";
import { useSubjectRows } from "@/lib/api/catalog";
import { ApiError, apiErrorMessage } from "@/lib/api/client";
import { useRecord, type RecordAddress } from "@/lib/api/live";
import { formatBytes, formatCount, formatRelative, formatTimestamp } from "@/lib/format";
import type { KafkaRecord, TopicDetail } from "@/lib/api/types";
import { cn } from "@/lib/utils";

import { DeleteRecordsDialog } from "./delete-records";
import { ProduceRecordSheet, duplicateDraft } from "./produce-record";
import { useRecordAddress } from "./record-address";
import { recordId } from "./record-id";
import { RecordJump } from "./record-jump";
import { RecordTable } from "./record-table";
import { SchemaPicker } from "./schema-picker";

export type RecordFilter = {
  term: string;
  rules: FilterRule[];
  schemaId: number | null;
};

export const EMPTY_FILTER: RecordFilter = { term: "", rules: [], schemaId: null };

function partitionField(topic: TopicDetail): FilterField<KafkaRecord> {
  return {
    id: "partition",
    label: "Partition",
    plural: "partitions",
    icon: Rows3Icon,
    options: topic.partitions.map((partition) => ({
      value: String(partition.id),
      label: String(partition.id),
      hint: formatCount(partition.retained),
    })),
    accessor: (record) => String(record.partition),
  };
}

export function filterPartitions(topic: TopicDetail, filter: RecordFilter) {
  const field = partitionField(topic);
  const rule = filter.rules.find((candidate) => candidate.id === field.id);
  if (!rule) return null;
  return selectedOptions(field, rule).map((option) => Number(option.value));
}

export type RecordSource = {
  records: KafkaRecord[];
  scope: string;
  obfuscated: boolean;
  loading: boolean;
  refreshing: boolean;
  stale?: boolean;
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
        An obfuscation rule hides fields on this topic. Hidden fields show as *** or as a kx: token,
        and equal values get equal tokens. Values the schema registry could not decode are hidden
        whole. Search sees this view, not the original data.
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
    meta: { align: "right", width: "4rem" },
    cell: ({ getValue }) => <span className="numeric text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Offset" className="justify-end" />
    ),
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Key" />,
    meta: { width: "12rem" },
    cell: ({ row }) => (
      <span
        className={cn(
          "block truncate font-mono",
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
    meta: { minWidth: "4rem" },
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
    meta: { align: "right", width: "5rem" },
    cell: ({ getValue }) => (
      <span className="numeric text-muted-foreground">{formatBytes(getValue())}</span>
    ),
  }),
  columnHelper.accessor("timestamp", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Timestamp" className="justify-end" />
    ),
    meta: { align: "right", width: "11rem" },
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
  filters?: readonly CustomFilter[];
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
  filters,
  actions,
  notice,
  emptyState,
}: RecordViewProps) {
  const { address, open, link } = useRecordAddress();
  const [expanded, setExpanded] = useState(false);
  const [cutting, setCutting] = useState(false);
  const [columnSizing, setColumnSizing] = useState<ColumnSizingState>({});
  const sheetRef = useRef<HTMLDivElement>(null);

  const { records, obfuscated } = source;
  const showSchemaPicker =
    filter.schemaId != null ||
    records.some((record) => record.value != null && record.schemaId == null);
  const listed =
    address == null
      ? null
      : (records.find(
          (record) => record.partition === address.partition && record.offset === address.offset,
        ) ?? null);
  const lookup = useRecord(cluster, topic.name, address, filter.schemaId, listed == null);
  const selectedRecord = listed ?? lookup.data?.record ?? null;
  const selectedObfuscated = listed ? obfuscated : (lookup.data?.obfuscated ?? false);
  const selectedSchemaId =
    selectedRecord?.value == null ? null : (selectedRecord.schemaId ?? filter.schemaId);
  const { canChange } = useAccess();
  const canDuplicate = !topic.internal && canChange(cluster, "PRODUCE");
  const duplicate = canDuplicate && selectedRecord ? duplicateDraft(selectedRecord) : null;
  const canDelete = !topic.internal && canChange(cluster, "DELETE_RECORDS");

  const fields = [partitionField(topic)];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {notice}

      <RecordTable
        key={source.scope}
        columns={columns}
        data={records}
        columnSizing={columnSizing}
        onColumnSizingChange={setColumnSizing}
        toolbar={
          <>
            <SearchField
              className="min-w-48 flex-1"
              value={filter.term}
              onChange={(event) => onFilterChange({ ...filter, term: event.target.value })}
              placeholder="Search key or value…"
            />

            <FilterBar
              fields={fields}
              value={filter.rules}
              onChange={(rules) => onFilterChange({ ...filter, rules })}
              custom={filters}
            />

            {showSchemaPicker ? (
              <SchemaPicker
                cluster={cluster}
                preferred={`${topic.name}-value`}
                value={filter.schemaId}
                onChange={(schemaId) => onFilterChange({ ...filter, schemaId })}
                label="Decode value with schema"
                placeholder="Decode with schema…"
                raw
              />
            ) : null}

            {obfuscated ? <ObfuscatedBadge /> : null}

            <div className="ml-auto flex items-center gap-2">
              <RecordJump topic={topic} onOpen={open} />
              {actions}
            </div>
          </>
        }
        getRowId={recordId}
        loading={source.loading}
        refreshing={source.refreshing}
        stale={source.stale}
        hasNextPage={source.pages?.hasNextPage}
        fetchNextPage={source.pages?.fetchNextPage}
        isFetchingNextPage={source.pages?.isFetchingNextPage}
        isFetchNextPageError={source.pages?.isFetchNextPageError}
        onRowClick={(record) => open({ partition: record.partition, offset: record.offset })}
        selectedKey={address ? recordId(address) : undefined}
        error={source.error}
        emptyState={emptyState}
      />

      <Sheet
        open={address !== null}
        onOpenChange={(next) => {
          if (!next) {
            open(null);
            setExpanded(false);
          }
        }}
      >
        <SheetContent
          ref={sheetRef}
          initialFocus={sheetRef}
          side="right"
          className={cn(
            "w-full gap-0 outline-none data-[side=right]:w-full",
            expanded
              ? "data-[side=right]:sm:max-w-[min(90vw,56rem)]"
              : "data-[side=right]:sm:max-w-2xl",
          )}
        >
          {address ? (
            <SheetHeader className="gap-1 border-b px-5 py-4 pr-12">
              <div className="flex min-w-0 items-center gap-2">
                <SheetTitle className="min-w-0 truncate font-mono text-sm font-medium">
                  <span className="text-muted-foreground">{topic.name}</span>
                  <span className="text-muted-foreground/60"> / </span>
                  {address.partition}
                  <span className="text-muted-foreground/60"> @ </span>
                  {address.offset}
                </SheetTitle>
                <CopyButton value={link(address)} label="Copy link to this record" />
                {canDuplicate ? (
                  <ProduceRecordSheet
                    cluster={cluster}
                    topic={topic}
                    draft={duplicate ?? undefined}
                    trigger={duplicate ? <IconButton label="Duplicate this record" /> : null}
                    onProduced={open}
                  >
                    <CopyPlusIcon />
                  </ProduceRecordSheet>
                ) : null}
                {canDelete ? (
                  <>
                    <IconButton
                      label="Delete the records before this one"
                      onClick={() => setCutting(true)}
                    >
                      <ListXIcon />
                    </IconButton>
                    <DeleteRecordsDialog
                      cluster={cluster}
                      topic={topic}
                      cut={{ partition: address.partition, before: address.offset }}
                      open={cutting}
                      onOpenChange={setCutting}
                    />
                  </>
                ) : null}
                {selectedObfuscated ? <ObfuscatedBadge /> : null}
              </div>
              <SheetDescription>
                {selectedRecord
                  ? `Produced ${formatRelative(selectedRecord.timestamp)}`
                  : lookup.isError
                    ? "Not available"
                    : "Loading…"}
              </SheetDescription>
            </SheetHeader>
          ) : null}
          {address && !selectedRecord ? (
            lookup.isError ? (
              <Empty className="py-10">
                <EmptyHeader>
                  <EmptyMedia variant="icon">
                    <SearchXIcon />
                  </EmptyMedia>
                  <EmptyTitle>Record not found</EmptyTitle>
                  <EmptyDescription>{missingRecord(lookup.error, address)}</EmptyDescription>
                </EmptyHeader>
              </Empty>
            ) : (
              <div className="flex flex-1 items-center justify-center">
                <Spinner />
              </div>
            )
          ) : null}
          {selectedRecord ? (
            <>
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

function missingRecord(error: unknown, { partition, offset }: RecordAddress) {
  if (error instanceof ApiError && error.code === "UNKNOWN_OFFSET") {
    return `Partition ${partition} has no record at offset ${offset}. Retention or compaction may have removed it, or the offset may hold a transaction marker.`;
  }
  if (error instanceof ApiError && error.code === "UNKNOWN_PARTITION") {
    return `This topic has no partition ${partition}.`;
  }
  return apiErrorMessage(error, "Failed to load this record.");
}

function SchemaLink({ cluster, topic, id }: { cluster: string; topic: string; id: number }) {
  const { data } = useSubjectRows(cluster);
  const matches = data?.rows.filter((row) => versionWithId(row, id) != null) ?? [];
  const subject = matches.find((row) => row.subject === `${topic}-value`) ?? matches[0];
  const version = subject && versionWithId(subject, id);

  if (subject == null || version == null) {
    return id;
  }

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Link
            to="/cluster/$cluster/schemas"
            params={{ cluster }}
            search={{ subject: subject.subject, version }}
            className="text-primary underline-offset-4 outline-none hover:underline focus-visible:underline"
          />
        }
      >
        {id}
      </TooltipTrigger>
      <TooltipContent>
        <span className="font-mono">{subject.subject}</span> · v{version}
      </TooltipContent>
    </Tooltip>
  );
}

function Meta({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="min-w-0 space-y-0.5">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="numeric truncate text-sm">{value}</dd>
    </div>
  );
}
