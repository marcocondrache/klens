import { CrownIcon, HardDriveIcon } from "lucide-react";
import { createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ConfigTable } from "@/components/config-table";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { Facts } from "@/components/facts";
import { PageHeader } from "@/components/page-header";
import { Pill, UsedValue } from "@/components/status";
import { TabCount } from "@/components/tab-count";
import { useAccess } from "@/hooks/use-access";
import { useBroker, useClusterHealth } from "@/lib/api/catalog";
import { useBrokerConfigs } from "@/lib/api/live";
import { useClusterName } from "@/lib/clusters";
import { formatBytes, formatNumber } from "@/lib/format";
import { nodeDetailSearch, nodeTab, searchDefaults } from "@/lib/route-search";
import { isLogDirsPending, usedShare } from "@/lib/storage";
import type { BrokerRow, LogDir } from "@/lib/api/types";

export const Route = createFileRoute("/cluster/$cluster/nodes_/$id")({
  validateSearch: nodeDetailSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(nodeDetailSearch))] },
  component: NodePage,
});

const columnHelper = createColumnHelper<DataTableFeatures, LogDir>();

function optionalBytes(bytes: number | null) {
  if (bytes === null) {
    return (
      <span className="text-muted-foreground/60" title="The broker does not report its volume size">
        —
      </span>
    );
  }
  return formatBytes(bytes);
}

const columns = columnHelper.columns([
  columnHelper.accessor("path", {
    header: "Path",
    cell: ({ row }) => {
      const dir = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="truncate font-mono">{dir.path}</span>
          {dir.error ? (
            <Pill tone="error" className="shrink-0" title={dir.error}>
              <HardDriveIcon />
              offline
            </Pill>
          ) : null}
          {dir.cordoned ? (
            <Pill tone="warn" className="shrink-0" title="Takes no new partitions">
              cordoned
            </Pill>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("replicaCount", {
    id: "partitions",
    header: "Partitions",
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  columnHelper.accessor("sizeBytes", {
    id: "size",
    header: "Size",
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => formatBytes(getValue()),
  }),
  columnHelper.accessor((dir) => dir.usableBytes ?? -1, {
    id: "free",
    header: "Free",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => optionalBytes(row.original.usableBytes),
  }),
  columnHelper.accessor((dir) => dir.totalBytes ?? -1, {
    id: "capacity",
    header: "Capacity",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => optionalBytes(row.original.totalBytes),
  }),
  columnHelper.accessor((dir) => usedShare(dir) ?? -1, {
    id: "used",
    header: "Used",
    meta: { align: "right", width: "6rem" },
    cell: ({ row }) => <UsedValue used={usedShare(row.original)} />,
  }),
]);

function BrokerFacts({ broker }: { broker: BrokerRow }) {
  return (
    <Facts>
      <span className="inline-flex items-center gap-1">
        <span className="font-mono">
          {broker.host}:{broker.port}
        </span>
        <CopyButton value={`${broker.host}:${broker.port}`} label="Copy address" />
      </span>
      <span>{formatNumber(broker.partitionCount)} partitions</span>
      <span>{formatNumber(broker.leaderCount)} leaders</span>
      {broker.sizeBytes === null ? null : <span>{formatBytes(broker.sizeBytes)} on disk</span>}
    </Facts>
  );
}

function NodePage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { id } = Route.useParams();
  const { tab: requested } = Route.useSearch();
  const brokerId = Number(id);
  const { can } = useAccess();
  const canConfigs = can(cluster, "CONFIGS");
  const tab = requested === "config" && !canConfigs ? "log-dirs" : requested;

  const { data: broker, isPending } = useBroker(cluster, brokerId);
  const { data: health } = useClusterHealth(cluster);
  const { data: configs = [], isPending: configsPending } = useBrokerConfigs(
    cluster,
    brokerId,
    tab === "config" && canConfigs,
  );
  const logDirsLane = health?.logDirs;

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title={`Broker ${brokerId}`}
        description={broker ? <BrokerFacts broker={broker} /> : null}
        badges={
          broker ? (
            <>
              {broker.controller ? (
                <Pill tone="brand">
                  <CrownIcon />
                  controller
                </Pill>
              ) : null}
              {broker.rack ? <Pill>{broker.rack}</Pill> : null}
            </>
          ) : null
        }
      />

      <Tabs
        value={tab}
        onValueChange={(value) => void navigate({ search: { tab: nodeTab(value) }, replace: true })}
        className="min-h-0 flex-1"
      >
        <TabsList
          variant="line"
          className="w-full shrink-0 justify-start gap-3 border-b [&>[data-slot=tabs-trigger]]:flex-none [&>[data-slot=tabs-trigger]]:after:bg-brand"
        >
          <TabsTrigger value="log-dirs">
            Log dirs
            <TabCount value={isPending ? undefined : broker?.logDirs.length} />
          </TabsTrigger>
          {canConfigs ? <TabsTrigger value="config">Configuration</TabsTrigger> : null}
        </TabsList>

        <TabsContent value="log-dirs" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={columns}
            data={broker?.logDirs ?? []}
            getRowId={(dir) => dir.path}
            loading={isPending || isLogDirsPending(logDirsLane)}
            defaultSort={{ id: "path", direction: "asc" }}
            emptyState={
              logDirsLane?.lastError
                ? `Log dirs are unavailable: ${logDirsLane.lastError}`
                : "This broker reports no log dirs."
            }
          />
        </TabsContent>

        {canConfigs ? (
          <TabsContent value="config" className="mt-4 flex min-h-0 flex-col">
            <ConfigTable entries={configs} loading={configsPending} />
          </TabsContent>
        ) : null}
      </Tabs>
    </div>
  );
}
