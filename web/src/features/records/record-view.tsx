import type { ReactNode } from "react";

import { FilterBar, type CustomFilter } from "@/components/data-table/filter-bar";
import { SearchField } from "@/components/search-field";
import type { TailStatus } from "@/lib/api/tail";
import type { KafkaRecord, TopicDetail } from "@/lib/api/types";

import { ObfuscatedBadge } from "./obfuscated-badge";
import { useRecordAddress } from "./record-address";
import { recordColumns } from "./record-columns";
import { partitionField, type RecordFilter } from "./record-filter";
import { recordId } from "./record-id";
import { RecordJump } from "./record-jump";
import { LiveToggle, RecordOrderMenu, type RecordMode } from "./record-mode";
import { RecordSheet } from "./record-sheet";
import { RecordTable } from "./record-table";
import { SchemaPicker } from "./schema-picker";

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

type RecordViewProps = {
  cluster: string;
  topic: TopicDetail;
  source: RecordSource;
  filter: RecordFilter;
  onFilterChange: (filter: RecordFilter) => void;
  filters?: readonly CustomFilter[];
  mode: RecordMode;
  onModeChange: (mode: RecordMode) => void;
  tailStatus?: TailStatus;
  status?: ReactNode;
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
  mode,
  onModeChange,
  tailStatus,
  status,
  actions,
  notice,
  emptyState,
}: RecordViewProps) {
  const { address, open } = useRecordAddress();

  const { records, obfuscated } = source;
  const showSchemaPicker =
    filter.schemaId != null ||
    records.some((record) => record.value != null && record.schemaId == null);
  const fields = [partitionField(topic)];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      <div className="flex shrink-0 flex-wrap items-center gap-2">
        <SearchField
          className="min-w-48 flex-1"
          value={filter.term}
          onChange={(event) => onFilterChange({ ...filter, term: event.target.value })}
          placeholder="Search key or value…"
        />

        <RecordOrderMenu value={mode} onChange={onModeChange} />

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

        {/* The jump and the live toggle keep one width in every mode, so they
            close the row and stay put while the mode's actions change. */}
        <div className="ml-auto flex items-center gap-2">
          {obfuscated ? <ObfuscatedBadge /> : null}
          {status}
          {actions}
          <RecordJump topic={topic} onOpen={open} />
          <LiveToggle value={mode} onChange={onModeChange} status={tailStatus} />
        </div>
      </div>

      {notice}

      <RecordTable
        key={source.scope}
        columns={recordColumns}
        data={records}
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

      <RecordSheet
        cluster={cluster}
        topic={topic}
        records={records}
        obfuscated={obfuscated}
        schemaId={filter.schemaId}
      />
    </div>
  );
}
