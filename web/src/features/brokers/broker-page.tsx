import { useState } from "react";
import { CrownIcon } from "lucide-react";
import { getRouteApi } from "@tanstack/react-router";

import { createDialogHandle } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ConfigTable } from "@/components/config-table";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table/data-table";
import { Facts } from "@/components/facts";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { TabCount } from "@/components/tab-count";
import { useAccess } from "@/hooks/use-access";
import { useBroker, useClusterHealth } from "@/lib/api/catalog";
import { useBrokerConfigs } from "@/lib/api/live";
import { useClusterName } from "@/lib/clusters";
import { formatBytes, formatNumber } from "@/lib/format";
import { isLogDirsPending } from "@/lib/storage";
import type { BrokerRow, ConfigEntry } from "@/lib/api/types";

import { brokerTab } from "./search";
import { logDirColumns } from "./broker-columns";
import { EditBrokerConfigButton, EditBrokerConfigDialog } from "./edit-broker-config";

const route = getRouteApi("/cluster/$cluster/nodes_/$id");

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

export function BrokerPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const { id } = route.useParams();
  const { tab: requested } = route.useSearch();
  const brokerId = Number(id);
  const { can, canChange } = useAccess();
  const canConfigs = can(cluster, "CONFIGS");
  const canAlter = canChange(cluster, "ALTER_BROKER_CONFIGS");
  const [configEditor] = useState(() => createDialogHandle<ConfigEntry>());
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
        onValueChange={(value) =>
          void navigate({ search: { tab: brokerTab(value) }, replace: true })
        }
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
            columns={logDirColumns}
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
            <ConfigTable
              entries={configs}
              loading={configsPending}
              action={
                canAlter
                  ? (entry) =>
                      entry.readOnly ? null : (
                        <EditBrokerConfigButton handle={configEditor} entry={entry} />
                      )
                  : undefined
              }
            />
            {canAlter ? (
              <EditBrokerConfigDialog cluster={cluster} broker={brokerId} handle={configEditor} />
            ) : null}
          </TabsContent>
        ) : null}
      </Tabs>
    </div>
  );
}
