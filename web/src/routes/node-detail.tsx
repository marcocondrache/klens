import {
  ArrowDownRightIcon,
  ArrowUpRightIcon,
  CrownIcon,
  DatabaseIcon,
  NetworkIcon,
} from "lucide-react";
import { useParams } from "react-router";

import { ConfigTable } from "@/components/config-table";
import { CopyButton } from "@/components/copy-button";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { Stat, StatGrid } from "@/components/stat";
import { useBroker, useBrokerConfigs } from "@/lib/api/queries";
import { useClusterName } from "@/lib/clusters";
import { formatBytes, formatNumber, formatRate } from "@/lib/format";

export function NodePage() {
  const cluster = useClusterName();
  const { id } = useParams<{ id: string }>();
  const brokerId = Number(id);

  const { data: broker, isPending } = useBroker(cluster, brokerId);
  const { data: configs = [], isPending: configsPending } = useBrokerConfigs(cluster, brokerId);

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
        <Stat
          label="Log size"
          value={formatBytes(broker?.logDirSizeBytes ?? 0)}
          hint="across all log dirs"
          icon={<DatabaseIcon />}
          loading={isPending}
        />
        <Stat
          label="Bytes in"
          value={formatRate(broker?.bytesInPerSec ?? 0)}
          icon={<ArrowUpRightIcon />}
          loading={isPending}
        />
        <Stat
          label="Bytes out"
          value={formatRate(broker?.bytesOutPerSec ?? 0)}
          icon={<ArrowDownRightIcon />}
          loading={isPending}
        />
      </StatGrid>

      <ConfigTable entries={configs} loading={configsPending} fill />
    </div>
  );
}
