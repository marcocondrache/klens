import type { ReactNode } from "react";
import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import type { ClientQuota } from "@/lib/api/types";
import { formatBytes, formatNumber, formatThroughput } from "@/lib/format";

import { EntityCell, entityKey } from "./quota-entity";

function quotaValue(
  value: number | null,
  display: (value: number) => string,
  exact: (value: number) => string,
) {
  if (value === null) {
    return <span className="text-muted-foreground/60">—</span>;
  }

  return (
    <span className="numeric" title={exact(value)}>
      {display(value)}
    </span>
  );
}

function byteRate(value: number | null) {
  return quotaValue(
    value,
    (bytes) => `${formatBytes(bytes)}/s`,
    (bytes) => `${formatNumber(bytes)} bytes/s`,
  );
}

function perSecond(value: number | null) {
  return quotaValue(
    value,
    (rate) => `${formatThroughput(rate)}/s`,
    (rate) => `${formatNumber(rate)}/s`,
  );
}

function percent(value: number | null) {
  return quotaValue(
    value,
    (share) => `${formatNumber(share)}%`,
    () => "Of network and I/O thread time",
  );
}

const columnHelper = createColumnHelper<DataTableFeatures, ClientQuota>();

function rateColumn(
  id: string,
  title: string,
  pick: (quota: ClientQuota) => number | null,
  render: (value: number | null) => ReactNode,
) {
  return columnHelper.accessor((quota) => pick(quota) ?? -1, {
    id,
    header: title,
    meta: { align: "right", width: "8rem" },
    cell: ({ row }) => render(pick(row.original)),
  });
}

export const quotaColumns = columnHelper.columns([
  columnHelper.accessor(entityKey, {
    id: "entity",
    header: "Entity",
    cell: ({ row }) => <EntityCell parts={row.original.entity} />,
  }),
  rateColumn("produce", "Produce", (quota) => quota.producerByteRate, byteRate),
  rateColumn("consume", "Consume", (quota) => quota.consumerByteRate, byteRate),
  rateColumn("request", "Request time", (quota) => quota.requestPercentage, percent),
  rateColumn("mutations", "Mutations", (quota) => quota.controllerMutationRate, perSecond),
  rateColumn("connections", "Connections", (quota) => quota.connectionCreationRate, perSecond),
]);
