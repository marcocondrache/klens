import { createRootRoute, createRoute, createRouter, redirect } from "@tanstack/react-router";

import App from "@/App";
import {
  parseGroupDetailSearch,
  parseGroupsSearch,
  parseLoginSearch,
  parseSchemasSearch,
  parseTopicDetailSearch,
  parseTopicsSearch,
} from "@/lib/route-search";
import { AppLayout } from "@/routes/app-layout";
import { ConsumerGroupPage } from "@/routes/group-detail";
import { ConsumerGroupsPage } from "@/routes/groups";
import { HomePage } from "@/routes/home";
import { LoginPage } from "@/routes/login";
import { NodePage } from "@/routes/node-detail";
import { NodesPage } from "@/routes/nodes";
import { NotFoundPage } from "@/routes/not-found";
import { SchemasPage } from "@/routes/schemas";
import { TopicPage } from "@/routes/topic-detail";
import { TopicsPage } from "@/routes/topics";

function parseSearch(searchStr: string): Record<string, string> {
  const query = searchStr.startsWith("?") ? searchStr.slice(1) : searchStr;
  const params = new URLSearchParams(query);
  const out: Record<string, string> = {};
  params.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}

function stringifySearch(search: Record<string, unknown>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(search)) {
    if (typeof value === "string") {
      if (value !== "") params.set(key, value);
      continue;
    }
    if (typeof value === "number" || typeof value === "boolean") {
      params.set(key, String(value));
    }
  }
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

const rootRoute = createRootRoute({
  component: App,
  notFoundComponent: NotFoundPage,
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: HomePage,
});

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  validateSearch: parseLoginSearch,
  component: LoginPage,
});

const clusterRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/cluster/$cluster",
  component: AppLayout,
  notFoundComponent: NotFoundPage,
});

const clusterIndexRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "/",
  beforeLoad: ({ params }) => {
    throw redirect({ href: `/cluster/${params.cluster}/topics` });
  },
});

const topicsRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "topics",
  validateSearch: parseTopicsSearch,
  component: TopicsPage,
});

const topicRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "topics/$topic",
  validateSearch: parseTopicDetailSearch,
  component: TopicPage,
});

const groupsRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "groups",
  validateSearch: parseGroupsSearch,
  component: ConsumerGroupsPage,
});

const groupRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "groups/$group",
  validateSearch: parseGroupDetailSearch,
  component: ConsumerGroupPage,
});

const schemasRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "schemas",
  validateSearch: parseSchemasSearch,
  component: SchemasPage,
});

const nodesRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "nodes",
  component: NodesPage,
});

const nodeRoute = createRoute({
  getParentRoute: () => clusterRoute,
  path: "nodes/$id",
  component: NodePage,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  loginRoute,
  clusterRoute.addChildren([
    clusterIndexRoute,
    topicsRoute,
    topicRoute,
    groupsRoute,
    groupRoute,
    schemasRoute,
    nodesRoute,
    nodeRoute,
  ]),
]);

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
