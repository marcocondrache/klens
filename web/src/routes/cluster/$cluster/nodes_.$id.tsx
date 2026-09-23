import { CrownIcon } from "lucide-react";
import { createFileRoute } from "@tanstack/react-router";

import { ConfigTable } from "@/components/config-table";
import { CopyButton } from "@/components/copy-button";
import { Facts } from "@/components/facts";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { useBroker } from "@/lib/api/catalog";
import { useBrokerConfigs } from "@/lib/api/live";
import { useAccess } from "@/hooks/use-access";
import { useClusterName } from "@/lib/clusters";
import { formatNumber } from "@/lib/format";
import type { BrokerRow } from "@/lib/api/types";

export const Route = createFileRoute("/cluster/$cluster/nodes_/$id")({
  component: NodePage,
});

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
    </Facts>
  );
}

function NodePage() {
  const cluster = useClusterName();
  const { id } = Route.useParams();
  const brokerId = Number(id);

  const { can } = useAccess();
  const canConfigs = can(cluster, "CONFIGS");
  const { data: broker } = useBroker(cluster, brokerId);
  const { data: configs = [], isPending: configsPending } = useBrokerConfigs(
    cluster,
    brokerId,
    canConfigs,
  );

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
