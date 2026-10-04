import { useId, useMemo } from "react";
import { getRouteApi } from "@tanstack/react-router";

import { Field, FieldLabel } from "@/components/ui/field";
import { Switch } from "@/components/ui/switch";
import { DataTable } from "@/components/data-table/data-table";
import { FilterBar } from "@/components/data-table/filter-bar";
import { useTableSearch } from "@/components/data-table/use-table-search";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { useAccess } from "@/hooks/use-access";
import { useClusterHealth, useTopicRows } from "@/lib/api/catalog";
import { apiErrorMessage } from "@/lib/api/client";
import type { TopicRow } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";

import { CreateTopicSheet } from "./create-topic";
import type { TopicsSearch } from "./search";
import { topicColumns } from "./topics-columns";
import { TOPIC_FILTERS, topicMatches } from "./topics-filters";

const route = getRouteApi("/cluster/$cluster/topics");

const EMPTY_TOPICS: TopicRow[] = [];

export function TopicsPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const search = route.useSearch();
  const showInternal = search.internal;
  const { canChange } = useAccess();
  const internalId = useId();

  function setSearch(patch: Partial<TopicsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }

  const { data: topics = EMPTY_TOPICS, isPending, isError, error } = useTopicRows(cluster);
  const { data: health } = useClusterHealth(cluster);

  const visible = useMemo(
    () => (showInternal ? topics : topics.filter((topic) => !topic.internal)),
    [topics, showInternal],
  );
  const { searchInput, rows, filterBar } = useTableSearch({
    rows: visible,
    fields: TOPIC_FILTERS,
    search,
    setSearch,
    matches: topicMatches,
  });

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Topics"
        description={
          <>
            {rows.length} of {topics.length} topics
            <LaneCaption lane={health?.topology} />
          </>
        }
      />

      <DataTable
        columns={topicColumns}
        data={rows}
        getRowId={(topic) => topic.name}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search topics…" />

            <FilterBar {...filterBar} />

            <Field orientation="horizontal" className="ml-auto w-fit">
              <Switch
                id={internalId}
                size="sm"
                checked={showInternal}
                onCheckedChange={(checked) => setSearch({ internal: checked })}
              />
              <FieldLabel htmlFor={internalId} className="text-sm font-normal">
                Show internal
              </FieldLabel>
            </Field>

            {canChange(cluster, "CREATE_TOPICS") ? <CreateTopicSheet cluster={cluster} /> : null}
          </>
        }
        loading={isPending}
        error={isError ? apiErrorMessage(error, "Failed to load topics.") : undefined}
        defaultSort={{ id: "name", direction: "asc" }}
        onRowClick={(topic) => {
          void navigate({
            to: "/cluster/$cluster/topics/$topic",
            params: { cluster, topic: topic.name },
          });
        }}
      />
    </div>
  );
}
