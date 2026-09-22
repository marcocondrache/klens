import { createRouter } from "@tanstack/react-router";

import { queryClient } from "@/lib/query-client";
import { parseSearch, stringifySearch } from "@/lib/route-search";
import { routeTree } from "@/routeTree.gen";

export const router = createRouter({
  routeTree,
  context: { queryClient },
  parseSearch,
  stringifySearch,
  scrollRestoration: false,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
