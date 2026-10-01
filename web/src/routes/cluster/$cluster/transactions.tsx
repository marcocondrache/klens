import { createFileRoute } from "@tanstack/react-router";

import { TransactionsPage } from "@/features/transactions/transactions-page";

export const Route = createFileRoute("/cluster/$cluster/transactions")({
  component: TransactionsPage,
});
