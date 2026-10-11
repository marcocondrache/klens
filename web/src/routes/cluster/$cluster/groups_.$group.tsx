import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { GroupPage } from "@/features/groups/group-page";
import { groupDetailSearch } from "@/features/groups/search";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/groups_/$group")({
  validateSearch: groupDetailSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(groupDetailSearch))] },
  component: GroupPage,
});
