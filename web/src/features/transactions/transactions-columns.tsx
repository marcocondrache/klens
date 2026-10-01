import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import { Pill, TONE_TEXT } from "@/components/status";
import type { OpenTransaction } from "@/lib/api/types";
import { formatDuration, formatEnumLabel } from "@/lib/format";
import { cn } from "@/lib/utils";

const columnHelper = createColumnHelper<DataTableFeatures, OpenTransaction>();

export function openColumns(now: number) {
  return columnHelper.columns([
    columnHelper.accessor("transactionalId", {
      header: "Transactional ID",
      cell: ({ row }) => (
        <span className="flex min-w-0 items-center gap-2">
          <span className="min-w-0 truncate font-mono">{row.original.transactionalId}</span>
          {row.original.pastTimeout ? (
            <Pill tone="warn" title="Open longer than its own timeout" className="shrink-0">
              past timeout
            </Pill>
          ) : null}
        </span>
      ),
    }),
    columnHelper.accessor("state", {
      header: "State",
      meta: { width: "8rem" },
      cell: ({ getValue }) => formatEnumLabel(getValue()),
    }),
    columnHelper.accessor("producerId", {
      header: "Producer",
      meta: { align: "right", width: "8rem" },
      cell: ({ row }) => (
        <span className="font-mono">
          {row.original.producerId}
          <span className="text-muted-foreground">:{row.original.producerEpoch}</span>
        </span>
      ),
    }),
    columnHelper.accessor((transaction) => age(transaction.startedAt, now) ?? -1, {
      id: "age",
      header: "Age",
      meta: { align: "right", width: "6rem" },
      cell: ({ getValue, row }) => {
        const value = getValue();
        if (value < 0) {
          return <span className="text-muted-foreground/60">—</span>;
        }
        return (
          <span className={cn(row.original.pastTimeout && TONE_TEXT.warn)}>
            {formatDuration(Math.max(value, 1_000))}
          </span>
        );
      },
    }),
    columnHelper.accessor("timeoutMs", {
      id: "timeout",
      header: "Timeout",
      meta: { align: "right", width: "6rem" },
      cell: ({ getValue }) => (
        <span className="text-muted-foreground">{formatDuration(getValue())}</span>
      ),
    }),
    columnHelper.accessor((transaction) => transaction.partitions.length, {
      id: "partitions",
      header: "Partitions",
      meta: { width: "18rem" },
      cell: ({ row }) => (
        <span className="flex flex-wrap gap-1">
          {partitionsByTopic(row.original).map(([topic, count]) => (
            <Pill key={topic} className="max-w-full font-mono font-normal text-foreground">
              <span className="truncate">{topic}</span>
              <span className="shrink-0 text-muted-foreground">×{count}</span>
            </Pill>
          ))}
        </span>
      ),
    }),
  ]);
}

function age(startedAt: string | null, now: number) {
  return startedAt === null ? null : now - new Date(startedAt).getTime();
}

function partitionsByTopic(transaction: OpenTransaction): Array<[string, number]> {
  const counts = new Map<string, number>();
  for (const { topic } of transaction.partitions) {
    counts.set(topic, (counts.get(topic) ?? 0) + 1);
  }
  return [...counts];
}
