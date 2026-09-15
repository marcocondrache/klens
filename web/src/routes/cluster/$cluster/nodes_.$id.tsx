import { CrownIcon, NetworkIcon } from "lucide-react";
import { createFileRoute } from "@tanstack/react-router";

import { ConfigTable } from "@/components/config-table";
import { CopyButton } from "@/components/copy-button";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { Stat, StatGrid } from "@/components/stat";
import { useBroker } from "@/lib/api/catalog";
import { useBrokerConfigs } from "@/lib/api/live";
import { useAccess } from "@/hooks/use-access";
import { useClusterName } from "@/lib/clusters";
import { formatNumber } from "@/lib/format";

export const Route = createFileRoute("/cluster/$cluster/nodes_/$id")({
  component: NodePage,
});

function NodePage() {
  const cluster = useClusterName();
  const { id } = Route.useParams();
  const brokerId = Number(id);

  const { can } = useAccess();
  const canConfigs = can(cluster, "configs");
  const { data: broker, isPending } = useBroker(cluster, brokerId);
  const { data: configs = [], isPending: configsPending } = useBrokerConfigs(
    cluster,
    brokerId,
    canConfigs,
  );

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title={`Broker ${brokerId}`}
        description={
          broker ? (
            <span className="flex items-center gap-1">
              <span className="font-mono text-sm">
                {broker.host}:{broker.port}
              </span>
              <CopyButton value={`${broker.host}:${broker.port}`} label="Copy address" />
            </span>
          ) : null
        }
        badges={
          broker ? (
            <>
              {broker.controller ? (
                <Pill tone="brand">
                  <CrownIcon className="size-3" />
                  controller
                </Pill>
              ) : null}
              {broker.rack ? <Pill>{broker.rack}</Pill> : null}
            </>
          ) : null
        }
      />

      <StatGrid>
        <Stat
          label="Partitions"
          value={formatNumber(broker?.partitionCount ?? 0)}
          hint={`${formatNumber(broker?.leaderCount ?? 0)} as leader`}
          icon={<NetworkIcon />}
          loading={isPending}
        />
      </StatGrid>

      {canConfigs ? (
        <ConfigTable entries={configs} loading={configsPending} fill />
      ) : (
        <p className="text-sm text-muted-foreground">
          Live broker configuration is not available for your role.
        </p>
      )}
    </div>
  );
}
