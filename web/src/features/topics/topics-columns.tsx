import type { ReactNode } from "react";
import { TriangleAlertIcon } from "lucide-react";
import { createColumnHelper } from "@tanstack/react-table";

import { type DataTableFeatures } from "@/components/data-table/features";
import { PendingValue, Pill, SizeValue } from "@/components/status";
import type { TopicRow } from "@/lib/api/types";
import {
  formatCleanupPolicy,
  formatDuration,
  formatNumber,
  formatThroughput,
  isCompactCleanup,
} from "@/lib/format";

function emptyMetric(value: number, display: ReactNode) {
  if (value === 0) {
    return <span className="text-muted-foreground/60">—</span>;
  }

  return display;
}

const columnHelper = createColumnHelper<DataTableFeatures, TopicRow>();

export const topicColumns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: "Topic",
    meta: { grow: true },
    cell: ({ row }) => {
      const topic = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="truncate font-mono">{topic.name}</span>
          {topic.internal ? <Pill className="shrink-0">internal</Pill> : null}
          {topic.underReplicated ? (
            <Pill tone="warn" className="shrink-0">
              <TriangleAlertIcon />
              under-replicated
            </Pill>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("partitionCount", {
    id: "partitions",
    header: "Parts",
    size: 72,
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("replicationFactor", {
    id: "replication",
    header: "RF",
    size: 56,
    meta: { align: "right" },
  }),
  columnHelper.accessor("retainedMessages", {
    id: "messages",
    header: "Messages",
    size: 120,
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatNumber(getValue())),
  }),
  columnHelper.accessor((topic) => topic.sizeBytes ?? -1, {
    id: "size",
    header: "Size",
    size: 96,
    meta: { align: "right" },
    cell: ({ row }) => <SizeValue bytes={row.original.sizeBytes} />,
  }),
  columnHelper.accessor("rate", {
    id: "rate",
    header: "Msg/s",
    size: 80,
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatThroughput(getValue())),
  }),
  columnHelper.accessor((topic) => topic.retentionMs ?? Number.POSITIVE_INFINITY, {
    id: "retention",
    header: "Retention",
    size: 96,
    meta: { align: "right" },
    cell: ({ row }) => {
      if (row.original.retentionMs === null) {
        return <PendingValue label="Fetching topic configs" className="ml-auto block" />;
      }
      return (
        <span className="text-muted-foreground">{formatDuration(row.original.retentionMs)}</span>
      );
    },
  }),
  columnHelper.accessor("cleanupPolicy", {
    id: "policy",
    header: "Policy",
    size: 112,
    meta: { align: "right" },
    cell: ({ getValue }) => {
      const policy = getValue();
      if (policy === null) {
        return <PendingValue label="Fetching topic configs" className="ml-auto block" />;
      }
      return (
        <span className={isCompactCleanup(policy) ? "text-foreground" : "text-muted-foreground"}>
          {formatCleanupPolicy(policy)}
        </span>
      );
    },
  }),
  columnHelper.accessor("groupCount", {
    id: "groups",
    header: "Groups",
    size: 88,
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), getValue()),
  }),
]);
