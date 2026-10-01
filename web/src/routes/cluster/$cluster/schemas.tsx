import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { SchemasPage } from "@/features/schemas/schemas-page";
import { schemasSearch } from "@/features/schemas/search";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/schemas")({
  validateSearch: schemasSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(schemasSearch))] },
  component: SchemasPage,
});
