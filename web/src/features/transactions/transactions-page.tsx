import { TriangleAlertIcon } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { DataTable } from "@/components/data-table/data-table";
import { Facts } from "@/components/facts";
import { PageHeader } from "@/components/page-header";
import { TONE_TEXT } from "@/components/status";
import { useNow } from "@/hooks/use-now";
import { useClusterHealth, useTransactions } from "@/lib/api/catalog";
import { apiErrorMessage } from "@/lib/api/client";
import { useClusterName } from "@/lib/clusters";
import { formatNumber } from "@/lib/format";
import { cn } from "@/lib/utils";

import { CoverageNotices } from "./coverage-notices";
import { HangingList } from "./hanging-list";
import { openColumns } from "./transactions-columns";

export function TransactionsPage() {
  const cluster = useClusterName();
  const now = useNow();
  const { data, isPending, isError, error } = useTransactions(cluster);
  const { data: health } = useClusterHealth(cluster);

  const lane = health?.transactions;
  const listing = lane != null && lane.updatedAt == null && lane.lastError == null;
  const open = data?.open ?? [];
  const hanging = data?.hanging ?? [];
  const coverage = data?.coverage ?? null;
  const hangingPartitions = new Set(hanging.map((entry) => `${entry.topic}-${entry.partition}`));

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Transactions"
        description={
          <Facts>
            <span>{open.length} open</span>
            {coverage ? (
              <span className={cn(hangingPartitions.size > 0 && TONE_TEXT.warn)}>
                {hangingPartitions.size} hanging partitions
              </span>
            ) : null}
            {coverage && coverage.uncheckedPartitions > 0 ? (
              <span className={TONE_TEXT.warn} title="A leader failed to describe them">
                {formatNumber(coverage.uncheckedPartitions)} partitions unchecked
              </span>
            ) : null}
          </Facts>
        }
      />

      <div className="flex shrink-0 flex-col gap-2 empty:hidden">
        {lane && !lane.healthy && lane.lastError ? (
          <Alert variant="destructive">
            <TriangleAlertIcon />
            <AlertTitle>Transactions are unavailable</AlertTitle>
            <AlertDescription>{lane.lastError}</AlertDescription>
          </Alert>
        ) : null}
        {coverage ? <CoverageNotices coverage={coverage} /> : null}
      </div>

      {hanging.length > 0 ? <HangingList cluster={cluster} hanging={hanging} now={now} /> : null}

      <section className="flex min-h-64 flex-1 flex-col gap-2">
        <h2 className="text-sm font-medium">Open transactions</h2>
        <DataTable
          columns={openColumns(now)}
          data={open}
          getRowId={(transaction) => transaction.transactionalId}
          loading={isPending || listing}
          error={isError ? apiErrorMessage(error, "Failed to load transactions.") : undefined}
          emptyState="No open transactions. Kafka lists only the transactional ids klens may describe."
          defaultSort={{ id: "age", direction: "desc" }}
        />
      </section>
    </div>
  );
}
