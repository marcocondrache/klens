import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { UsersPage } from "@/features/users/users-page";
import { usersSearch } from "@/features/users/search";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/users")({
  validateSearch: usersSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(usersSearch))] },
  component: UsersPage,
});
