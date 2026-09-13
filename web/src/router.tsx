import { createRouter } from "@tanstack/react-router";

import { parseSearch, stringifySearch } from "@/lib/route-search";
import { routeTree } from "@/routeTree.gen";

export const router = createRouter({
  routeTree,
  parseSearch,
  stringifySearch,
  scrollRestoration: false,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
