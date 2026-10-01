import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { AclsPage } from "@/features/acls/acls-page";
import { aclsSearch } from "@/features/acls/search";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/acls")({
  validateSearch: aclsSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(aclsSearch))] },
  component: AclsPage,
});
