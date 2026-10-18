import { createColumnHelper } from "@tanstack/react-table";

import { type DataTableFeatures } from "@/components/data-table/features";
import { GroupStateBadge, LagValue, Pill, SizeValue } from "@/components/status";
import type { PartitionRow, TopicGroupRow } from "@/lib/api/types";
import { formatNumber } from "@/lib/format";

const partitionColumnHelper = createColumnHelper<DataTableFeatures, PartitionRow>();
const groupColumnHelper = createColumnHelper<DataTableFeatures, TopicGroupRow>();

export const partitionColumns = partitionColumnHelper.columns([
  partitionColumnHelper.accessor("id", {
    header: "Partition",
    size: 96,
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("leader", {
    header: "Leader",
    size: 80,
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("replicas", {
    header: "Replicas",
    meta: { grow: true },
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.replicas.map((replica) => (
          <Pill
            key={replica}
            tone={row.original.isr.includes(replica) ? "idle" : "error"}
            className="numeric min-w-5 justify-center"
            title={row.original.isr.includes(replica) ? "In sync" : "Out of sync"}
          >
            {replica}
          </Pill>
        ))}
      </span>
    ),
  }),
  partitionColumnHelper.accessor((partition) => partition.isr.length, {
    id: "isr",
    header: "In sync",
    size: 80,
    meta: { align: "right" },
    cell: ({ row }) => (
      <span
        className={
          row.original.isr.length < row.original.replicas.length
            ? "numeric text-warn"
            : "numeric text-muted-foreground"
        }
      >
        {row.original.isr.length}/{row.original.replicas.length}
      </span>
    ),
  }),
  partitionColumnHelper.accessor("lowWatermark", {
    id: "low",
    header: "Low offset",
    size: 128,
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor("highWatermark", {
    id: "high",
    header: "High offset",
    size: 128,
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor("retained", {
    id: "messages",
    header: "Messages",
    size: 128,
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor((partition) => partition.sizeBytes ?? -1, {
    id: "size",
    header: "Size",
    size: 104,
    meta: { align: "right" },
    cell: ({ row }) => <SizeValue bytes={row.original.sizeBytes} />,
  }),
]);

export const topicGroupColumns = groupColumnHelper.columns([
  groupColumnHelper.accessor("id", {
    header: "Group",
    meta: { grow: true },
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  groupColumnHelper.accessor("state", {
    header: "State",
    size: 176,
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  groupColumnHelper.accessor("memberCount", {
    id: "members",
    header: "Members",
    size: 96,
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  groupColumnHelper.accessor((group) => group.lagOnTopic ?? -1, {
    id: "lag",
    header: "Lag on this topic",
    size: 144,
    meta: { align: "right" },
    cell: ({ row }) => <LagValue lag={row.original.lagOnTopic} />,
  }),
]);
