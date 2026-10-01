import { TriangleAlertIcon } from "lucide-react";
import { Link } from "@tanstack/react-router";

import { Facts } from "@/components/facts";
import { Pill } from "@/components/status";
import type { HangingPartition, HangingReason } from "@/lib/api/types";
import { formatNumber, formatRelative } from "@/lib/format";

const REASON_LABEL: Record<HangingReason, string> = {
  PAST_TIMEOUT: "past max timeout",
  UNKNOWN_TO_COORDINATOR: "unknown to coordinator",
};

export function HangingList({
  cluster,
  hanging,
  now,
}: {
  cluster: string;
  hanging: HangingPartition[];
  now: number;
}) {
  return (
    <section className="flex shrink-0 flex-col gap-2">
      <h2 className="text-sm font-medium">Hanging transactions</h2>
      <ul className="max-h-96 divide-y overflow-y-auto rounded-lg border border-warn/40">
        {hanging.map((entry) => (
          <li
            key={`${entry.topic}-${entry.partition}-${entry.producerId}`}
            className="flex flex-col gap-1.5 px-4 py-3"
          >
            <div className="flex flex-wrap items-center gap-2">
              <TriangleAlertIcon className="size-4 text-warn" />
              <Link
                to="/cluster/$cluster/topics/$topic"
                params={{ cluster, topic: entry.topic }}
                className="font-mono font-medium"
              >
                {entry.topic}
              </Link>
              <span className="numeric text-muted-foreground">partition {entry.partition}</span>
              <Pill tone="warn">{REASON_LABEL[entry.reason]}</Pill>
            </div>
            <p>
              Blocks <code className="font-mono text-[0.9em]">read_committed</code> consumers at
              offset <span className="numeric font-medium">{formatNumber(entry.offset)}</span>.
            </p>
            <div className="text-sm text-muted-foreground">
              <Facts>
                <span>
                  producer <span className="font-mono">{entry.producerId}</span>
                  <span className="font-mono">:{entry.producerEpoch}</span>
                </span>
                {entry.transactionalId ? (
                  <span className="font-mono">{entry.transactionalId}</span>
                ) : (
                  <span>no transactional id</span>
                )}
                {entry.openSince ? (
                  <span>open since {formatRelative(entry.openSince, now)}</span>
                ) : null}
              </Facts>
            </div>
            <div className="flex flex-wrap items-center gap-1 text-sm">
              {entry.groups.length === 0 ? (
                <span className="text-muted-foreground">No consumer group reads this topic.</span>
              ) : (
                <>
                  <span className="text-muted-foreground">Read by</span>
                  {entry.groups.map((group) => (
                    <Link
                      key={group}
                      to="/cluster/$cluster/groups/$group"
                      params={{ cluster, group }}
                      className="font-mono"
                    >
                      <Pill className="font-normal text-foreground">{group}</Pill>
                    </Link>
                  ))}
                </>
              )}
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}
