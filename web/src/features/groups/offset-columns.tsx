import { Link } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import { PendingValue } from "@/components/status";
import { formatNumber } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { GroupOffset } from "@/lib/api/types";

const columnHelper = createColumnHelper<DataTableFeatures, GroupOffset>();

function OffsetValue({ value, label }: { value: number | null; label: string }) {
  if (value === null) {
    return <PendingValue label={label} className="ml-auto block" />;
  }
  return <span className="text-muted-foreground">{formatNumber(value)}</span>;
}

/** `maxLag` scales the lag bars, and `memberLabels` names each member by its client ID. */
export function offsetColumns({
  cluster,
  maxLag,
  memberLabels,
}: {
  cluster: string;
  maxLag: number;
  memberLabels: ReadonlyMap<string, string>;
}) {
  return columnHelper.columns([
    columnHelper.accessor("topic", {
      header: "Topic",
      cell: ({ getValue }) => (
        <Link
          to="/cluster/$cluster/topics/$topic"
          params={{ cluster, topic: getValue() }}
          className="font-mono outline-none"
          onClick={(event) => event.stopPropagation()}
        >
          {getValue()}
        </Link>
      ),
    }),
    columnHelper.accessor("partition", {
      header: "Partition",
      meta: { align: "right", width: "6rem" },
      cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
    }),
    columnHelper.accessor((offset) => offset.currentOffset ?? -1, {
      id: "current",
      header: "Committed",
      meta: { align: "right", width: "9rem" },
      cell: ({ row }) => (
        <OffsetValue value={row.original.currentOffset} label="Fetching committed offsets" />
      ),
    }),
    columnHelper.accessor((offset) => offset.endOffset ?? -1, {
      id: "end",
      header: "End offset",
      meta: { align: "right", width: "9rem" },
      cell: ({ row }) => (
        <OffsetValue value={row.original.endOffset} label="Sampling the watermark" />
      ),
    }),
    columnHelper.accessor((offset) => offset.lag ?? -1, {
      id: "lag",
      header: "Lag",
      meta: { align: "right", width: "12rem" },
      cell: ({ getValue, row }) => {
        if (row.original.lag === null) {
          return <PendingValue label="Lag is not known yet" className="ml-auto block" />;
        }
        const lag = getValue();

        return (
          <span className="flex items-center justify-end gap-3">
            <span className="h-1 w-20 overflow-hidden rounded-full bg-muted">
              <span
                className={cn(
                  "block h-full rounded-full",
                  lag > maxLag / 2 ? "bg-destructive/80" : "bg-foreground/35",
                )}
                style={{
                  width: `${Math.max(lag === 0 ? 0 : 3, (lag / maxLag) * 100)}%`,
                }}
              />
            </span>
            <span className={cn("numeric w-16", lag === 0 && "text-muted-foreground")}>
              {formatNumber(lag)}
            </span>
          </span>
        );
      },
    }),
    columnHelper.accessor((offset) => offset.memberId ?? "", {
      id: "member",
      header: "Member",
      meta: { align: "right", width: "14rem" },
      cell: ({ row }) =>
        row.original.memberId ? (
          <span className="font-mono text-muted-foreground">
            {memberLabels.get(row.original.memberId) ?? row.original.memberId}
          </span>
        ) : (
          <span className="text-muted-foreground/70">unassigned</span>
        ),
    }),
  ]);
}
