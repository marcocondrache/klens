import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { QuotasPage } from "@/features/quotas/quotas-page";
import { quotasSearch } from "@/features/quotas/search";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/quotas")({
  validateSearch: quotasSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(quotasSearch))] },
  component: QuotasPage,
});
