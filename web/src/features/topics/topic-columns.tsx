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
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("leader", {
    header: "Leader",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("replicas", {
    header: "Replicas",
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
    meta: { align: "right", width: "6rem" },
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
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor("highWatermark", {
    id: "high",
    header: "High offset",
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor("retained", {
    id: "messages",
    header: "Messages",
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor((partition) => partition.sizeBytes ?? -1, {
    id: "size",
    header: "Size",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => <SizeValue bytes={row.original.sizeBytes} />,
  }),
]);

export const topicGroupColumns = groupColumnHelper.columns([
  groupColumnHelper.accessor("id", {
    header: "Group",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  groupColumnHelper.accessor("state", {
    header: "State",
    meta: { width: "12rem" },
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  groupColumnHelper.accessor("memberCount", {
    id: "members",
    header: "Members",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => getValue(),
  }),
  groupColumnHelper.accessor((group) => group.lagOnTopic ?? -1, {
    id: "lag",
    header: "Lag on this topic",
    meta: { align: "right", width: "9rem" },
    cell: ({ row }) => <LagValue lag={row.original.lagOnTopic} />,
  }),
]);
