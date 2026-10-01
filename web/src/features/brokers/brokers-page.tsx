import { getRouteApi } from "@tanstack/react-router";

import { DataTable } from "@/components/data-table/data-table";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { useBrokerRows, useClusterHealth } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import type { BrokerRow } from "@/lib/api/types";

import { brokerColumns } from "./brokers-columns";

const route = getRouteApi("/cluster/$cluster/nodes");

export function BrokersPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const { data: brokers = [], isPending, isError, error } = useBrokerRows(cluster);
  const { data: health } = useClusterHealth(cluster);

  function openBroker(broker: BrokerRow) {
    void navigate({
      to: "/cluster/$cluster/nodes/$id",
      params: { cluster, id: String(broker.id) },
    });
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Brokers"
        description={
          <>
            {brokers.length} brokers
            <LaneCaption lane={health?.topology} />
          </>
        }
      />

      <DataTable
        columns={brokerColumns}
        data={brokers}
        getRowId={(broker) => String(broker.id)}
        loading={isPending}
        error={isError ? apiErrorMessage(error, "Failed to load brokers.") : undefined}
        defaultSort={{ id: "id", direction: "asc" }}
        onRowClick={openBroker}
      />
    </div>
  );
}
