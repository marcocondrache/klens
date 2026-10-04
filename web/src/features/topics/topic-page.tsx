import { useState } from "react";
import { AlertTriangleIcon, ListXIcon, PlusIcon, SendIcon, Trash2Icon } from "lucide-react";
import { getRouteApi } from "@tanstack/react-router";

import { Button } from "@/components/ui/button";
import { createDialogHandle } from "@/components/ui/dialog";
import { DropdownMenuItem, DropdownMenuSeparator } from "@/components/ui/dropdown-menu";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ActionsMenu, useOpenDialog } from "@/components/actions-menu";
import { ConfigTable } from "@/components/config-table";
import { ConfirmDelete } from "@/components/confirm-delete";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table/data-table";
import { Facts } from "@/components/facts";
import { PageHeader } from "@/components/page-header";
import { DeleteRecordsDialog } from "@/features/records/delete-records";
import { ProduceRecordSheet } from "@/features/records/produce-record";
import { RecordBrowser } from "@/features/records/record-browser";
import { PendingValue, Pill, StatusDot } from "@/components/status";
import { TabCount } from "@/components/tab-count";
import { useAccess } from "@/hooks/use-access";
import { useTopic, useTopicGroups } from "@/lib/api/catalog";
import { clusterPathname, del } from "@/lib/api/client";
import { useTopicConfigs } from "@/lib/api/live";
import type { ConfigEntry, TopicDetail } from "@/lib/api/types";
import { catalogLookupMessage } from "@/lib/catalog-lookup";
import { useClusterName } from "@/lib/clusters";
import {
  formatBytes,
  formatCleanupPolicy,
  formatCount,
  formatDuration,
  formatThroughput,
} from "@/lib/format";

import { AddPartitionsDialog } from "./add-partitions";
import { EditTopicConfigSheet } from "./edit-topic-config";
import { topicTab } from "./search";
import { partitionColumns, topicGroupColumns } from "./topic-columns";

const route = getRouteApi("/cluster/$cluster/topics_/$topic");

function TopicFacts({ detail }: { detail: TopicDetail }) {
  return (
    <Facts>
      <span>{detail.partitions.length} partitions</span>
      <span>{formatCount(detail.retainedMessages)} messages</span>
      {detail.sizeBytes === null ? null : (
        <span title="One replica of each partition">{formatBytes(detail.sizeBytes)}</span>
      )}
      {detail.diskBytes === null || detail.diskBytes === detail.sizeBytes ? null : (
        <span title="Every replica">{formatBytes(detail.diskBytes)} on disk</span>
      )}
      {detail.retentionMs === null ? (
        <PendingValue label="Fetching topic configs" />
      ) : (
        <span>{formatDuration(detail.retentionMs)} retention</span>
      )}
      {detail.rate > 0 ? (
        <span className="inline-flex items-center gap-1.5 text-foreground">
          <StatusDot tone="brand" pulse />
          {formatThroughput(detail.rate)} msg/s
        </span>
      ) : (
        <span>idle</span>
      )}
    </Facts>
  );
}

export function TopicPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const { topic: topicName } = route.useParams();
  const { tab: tabParam } = route.useSearch();
  const { can, canChange } = useAccess();
  const canRecords = can(cluster, "RECORDS");
  const canConfigs = can(cluster, "TOPIC_CONFIGS");
  const requested = tabParam ?? (canRecords ? "data" : "partitions");
  const tab =
    (requested === "data" && !canRecords) || (requested === "config" && !canConfigs)
      ? "partitions"
      : requested;

  const { data: detail = null, isPending, isError, error } = useTopic(cluster, topicName);
  const { data: configs = [], isPending: configsPending } = useTopicConfigs(
    cluster,
    topicName,
    tab === "config" && canConfigs,
  );
  const { data: groups = [], isPending: groupsPending } = useTopicGroups(
    cluster,
    topicName,
    tab === "groups",
  );
  const [configEditor] = useState(() => createDialogHandle<ConfigEntry>());
  const dialogs = useOpenDialog<"partitions" | "records" | "delete">();

  const lookup = catalogLookupMessage({
    isPending,
    isError,
    error,
    data: detail,
    missing: "This topic does not exist in the selected cluster.",
    failed: "Failed to load this topic.",
  });
  if (lookup) {
    return <PageHeader title={topicName} mono description={lookup} />;
  }

  const editable = detail !== null && !detail.internal;
  const canProduce = editable && canChange(cluster, "PRODUCE");
  const canDeleteRecords = editable && canChange(cluster, "DELETE_RECORDS");
  const canAddPartitions = editable && canChange(cluster, "ADD_PARTITIONS");
  const canDeleteTopic = editable && canChange(cluster, "DELETE_TOPICS");
  const canAlterConfigs = editable && canChange(cluster, "ALTER_TOPIC_CONFIGS");

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title={
          <span className="flex items-center gap-1">
            {topicName}
            <CopyButton value={topicName} label="Copy topic name" size="icon-sm" />
          </span>
        }
        mono
        badges={
          detail ? (
            <>
              {detail.internal ? <Pill>internal</Pill> : null}
              <Pill>{formatCleanupPolicy(detail.cleanupPolicy)}</Pill>
              <Pill>RF {detail.replicationFactor}</Pill>
              {detail.underReplicated ? (
                <Pill tone="warn">
                  <AlertTriangleIcon />
                  under-replicated
                </Pill>
              ) : null}
            </>
          ) : null
        }
        description={detail ? <TopicFacts detail={detail} /> : null}
        actions={
          detail ? (
            <>
              {canProduce ? (
                <ProduceRecordSheet
                  cluster={cluster}
                  topic={detail}
                  trigger={<Button variant="outline" />}
                  onProduced={
                    canRecords
                      ? ({ partition, offset }) => void navigate({ search: { partition, offset } })
                      : undefined
                  }
                >
                  <SendIcon data-icon="inline-start" />
                  Produce record
                </ProduceRecordSheet>
              ) : null}
              {canAddPartitions || canDeleteRecords || canDeleteTopic ? (
                <ActionsMenu label="Topic actions">
                  {canAddPartitions ? (
                    <DropdownMenuItem onClick={() => dialogs.show("partitions")}>
                      <PlusIcon />
                      Add partitions
                    </DropdownMenuItem>
                  ) : null}
                  {canDeleteRecords ? (
                    <DropdownMenuItem onClick={() => dialogs.show("records")}>
                      <ListXIcon />
                      Delete records
                    </DropdownMenuItem>
                  ) : null}
                  {canDeleteTopic ? (
                    <>
                      {canAddPartitions || canDeleteRecords ? <DropdownMenuSeparator /> : null}
                      <DropdownMenuItem
                        variant="destructive"
                        onClick={() => dialogs.show("delete")}
                      >
                        <Trash2Icon />
                        Delete topic
                      </DropdownMenuItem>
                    </>
                  ) : null}
                </ActionsMenu>
              ) : null}
            </>
          ) : null
        }
      />

      {detail && canAddPartitions ? (
        <AddPartitionsDialog cluster={cluster} topic={detail} {...dialogs.props("partitions")} />
      ) : null}
      {detail && canDeleteRecords ? (
        <DeleteRecordsDialog cluster={cluster} topic={detail} {...dialogs.props("records")} />
      ) : null}
      {canDeleteTopic ? (
        <ConfirmDelete
          {...dialogs.props("delete")}
          noun="topic"
          name={topicName}
          consequence={
            <>
              This deletes <span className="font-mono">{topicName}</span> and every record in it,
              and cannot be undone.
            </>
          }
          onDelete={() => del(clusterPathname(cluster, "topics", encodeURIComponent(topicName)))}
          onDeleted={() => void navigate({ to: "/cluster/$cluster/topics", params: { cluster } })}
        />
      ) : null}

      <Tabs
        value={tab}
        onValueChange={(value) =>
          void navigate({ search: { tab: topicTab(value) }, replace: true })
        }
        className="min-h-0 flex-1"
      >
        <TabsList
          variant="line"
          className="w-full shrink-0 justify-start gap-3 border-b [&>[data-slot=tabs-trigger]]:flex-none [&>[data-slot=tabs-trigger]]:after:bg-brand"
        >
          {canRecords ? <TabsTrigger value="data">Data</TabsTrigger> : null}
          <TabsTrigger value="partitions">
            Partitions
            <TabCount value={detail?.partitions.length} />
          </TabsTrigger>
          <TabsTrigger value="groups">
            Consumer groups
            <TabCount value={detail?.groupCount} />
          </TabsTrigger>
          {canConfigs ? <TabsTrigger value="config">Configuration</TabsTrigger> : null}
        </TabsList>

        {canRecords ? (
          <TabsContent value="data" className="mt-4 flex min-h-0 flex-col">
            {detail ? <RecordBrowser cluster={cluster} topic={detail} /> : null}
          </TabsContent>
        ) : null}

        <TabsContent value="partitions" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={partitionColumns}
            data={detail?.partitions ?? []}
            getRowId={(partition) => String(partition.id)}
            loading={isPending}
            defaultSort={{ id: "id", direction: "asc" }}
          />
        </TabsContent>

        <TabsContent value="groups" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={topicGroupColumns}
            data={groups}
            getRowId={(group) => group.id}
            loading={groupsPending}
            onRowClick={(group) => {
              void navigate({
                to: "/cluster/$cluster/groups/$group",
                params: { cluster, group: group.id },
              });
            }}
            emptyState="No consumer group is subscribed to this topic."
          />
        </TabsContent>

        {canConfigs ? (
          <TabsContent value="config" className="mt-4 flex min-h-0 flex-col">
            <ConfigTable
              entries={configs}
              loading={configsPending}
              onEdit={canAlterConfigs ? (entry) => configEditor.openWithPayload(entry) : undefined}
            />
            {canAlterConfigs ? (
              <EditTopicConfigSheet cluster={cluster} topic={topicName} handle={configEditor} />
            ) : null}
          </TabsContent>
        ) : null}
      </Tabs>
    </div>
  );
}
