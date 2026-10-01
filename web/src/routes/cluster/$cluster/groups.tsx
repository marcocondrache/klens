import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { ConsumerGroupsPage } from "@/features/groups/groups-page";
import { groupsSearch } from "@/features/groups/search";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/groups")({
  validateSearch: groupsSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(groupsSearch))] },
  component: ConsumerGroupsPage,
});
