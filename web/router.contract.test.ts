import { createMemoryHistory, createRouter } from "@tanstack/react-router";

import {
  parseGroupDetailSearch,
  parseGroupsSearch,
  parseLoginSearch,
  parseSchemasSearch,
  parseSearch,
  parseTopicDetailSearch,
  parseTopicsSearch,
  stringifySearch,
} from "./src/lib/route-search";
import { routeTree } from "./src/routeTree.gen";

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEqual<T>(actual: T, expected: T, message: string) {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  assert(left === right, `${message}: ${left} !== ${right}`);
}

assertEqual(parseSearch(""), {}, "empty search parses to {}");
assertEqual(parseSearch("?"), {}, "lone ? parses to {}");
assertEqual(
  parseSearch("?q=orders&internal=1"),
  { q: "orders", internal: "1" },
  "flat query stays strings",
);
assertEqual(parseSearch("q=orders"), { q: "orders" }, "missing ? still parses");
assertEqual(parseSearch("?q=a&q=b"), { q: "b" }, "duplicate keys keep the last value");
assertEqual(parseSearch("?q="), { q: "" }, "empty string values survive parse");

assertEqual(stringifySearch({}), "", "empty object has no query string");
assertEqual(
  stringifySearch({ q: "orders", internal: "1" }),
  "?q=orders&internal=1",
  "strings stringify flat",
);
assertEqual(stringifySearch({ q: "" }), "", "empty strings are omitted");
assertEqual(stringifySearch({ n: 3, ok: true }), "?n=3&ok=true", "numbers and booleans stringify");
assertEqual(
  stringifySearch({ q: "orders", skip: { nested: true }, gone: null }),
  "?q=orders",
  "objects and null are omitted",
);

assertEqual(
  parseTopicsSearch({ q: "orders", internal: "1", policy: "delete", extra: "x" }),
  { q: "orders", internal: "1", policy: "delete" },
  "topics keeps known keys only",
);
assertEqual(
  parseTopicsSearch({ q: "", internal: "yes", policy: "all" }),
  {},
  "topics drops empty and invalid values",
);
assertEqual(
  parseTopicsSearch({ policy: "compact" }),
  { policy: "compact" },
  "topics accepts compact",
);

assertEqual(
  parseGroupsSearch({ q: "cg", state: "STABLE", extra: 1 }),
  { q: "cg", state: "STABLE" },
  "groups keeps known keys only",
);
assertEqual(parseGroupsSearch({ state: "unknown" }), {}, "groups drops unknown state");
assertEqual(parseGroupsSearch({ state: "DEAD" }), { state: "DEAD" }, "groups accepts DEAD");

assertEqual(parseSchemasSearch({ q: "payment", extra: "x" }), { q: "payment" }, "schemas keeps q");
assertEqual(parseSchemasSearch({ q: "" }), {}, "schemas drops empty q");

assertEqual(
  parseLoginSearch({ error: "auth", extra: "x" }),
  { error: "auth" },
  "login keeps error",
);
assertEqual(parseLoginSearch({ error: "" }), {}, "login drops empty error");

assertEqual(
  parseTopicDetailSearch({ tab: "partitions" }),
  { tab: "partitions" },
  "topic tab partitions",
);
assertEqual(parseTopicDetailSearch({ tab: "groups" }), { tab: "groups" }, "topic tab groups");
assertEqual(parseTopicDetailSearch({ tab: "config" }), { tab: "config" }, "topic tab config");
assertEqual(
  parseTopicDetailSearch({ tab: "data" }),
  {},
  "topic data tab is implied by omitting tab",
);
assertEqual(parseTopicDetailSearch({ tab: "nope" }), {}, "topic drops unknown tab");

assertEqual(parseGroupDetailSearch({ tab: "members" }), { tab: "members" }, "group tab members");
assertEqual(
  parseGroupDetailSearch({ tab: "offsets" }),
  {},
  "group offsets tab is implied by omitting tab",
);

async function locationAfter(entry: string) {
  const router = createRouter({
    routeTree,
    history: createMemoryHistory({ initialEntries: [entry] }),
    parseSearch,
    stringifySearch,
    scrollRestoration: false,
  });
  await router.load();
  const { pathname, searchStr } = router.state.location;
  return {
    pathname,
    searchStr,
    ids: router.state.matches.map((match) => match.routeId),
  };
}

const login = await locationAfter("/login?error=auth&extra=1");
assertEqual(login.pathname, "/login", "login path");
assertEqual(login.searchStr, "?error=auth&extra=1", "login keeps a flat query string");
assertEqual(login.ids, ["__root__", "/login"], "login match ids");

const topics = await locationAfter("/cluster/local/topics?q=orders&internal=1&policy=delete");
assertEqual(topics.pathname, "/cluster/local/topics", "topics path");
assertEqual(
  topics.searchStr,
  "?q=orders&internal=1&policy=delete",
  "topics search stays flat strings",
);
assertEqual(
  topics.ids,
  ["__root__", "/cluster/$cluster", "/cluster/$cluster/topics"],
  "topics match ids",
);

const topic = await locationAfter("/cluster/local/topics/orders?tab=partitions");
assertEqual(topic.pathname, "/cluster/local/topics/orders", "topic detail path");
assertEqual(topic.searchStr, "?tab=partitions", "topic tab search");
assertEqual(
  topic.ids,
  ["__root__", "/cluster/$cluster", "/cluster/$cluster/topics_/$topic"],
  "topic detail match ids",
);

const topicData = await locationAfter("/cluster/local/topics/orders?tab=data");
assertEqual(topicData.pathname, "/cluster/local/topics/orders", "topic data tab still matches");
assertEqual(topicData.searchStr, "?tab=data", "unknown tab stays in the URL");

const groups = await locationAfter("/cluster/local/groups?q=cg&state=EMPTY");
assertEqual(groups.pathname, "/cluster/local/groups", "groups path");
assertEqual(groups.searchStr, "?q=cg&state=EMPTY", "groups search stays flat");
assertEqual(
  groups.ids,
  ["__root__", "/cluster/$cluster", "/cluster/$cluster/groups"],
  "groups match ids",
);

const group = await locationAfter("/cluster/local/groups/payments?tab=members");
assertEqual(group.pathname, "/cluster/local/groups/payments", "group detail path");
assertEqual(group.searchStr, "?tab=members", "group tab search");
assertEqual(
  group.ids,
  ["__root__", "/cluster/$cluster", "/cluster/$cluster/groups_/$group"],
  "group detail match ids",
);

const schemas = await locationAfter("/cluster/local/schemas?q=payment");
assertEqual(schemas.pathname, "/cluster/local/schemas", "schemas path");
assertEqual(schemas.searchStr, "?q=payment", "schemas search");
assertEqual(
  schemas.ids,
  ["__root__", "/cluster/$cluster", "/cluster/$cluster/schemas"],
  "schemas match ids",
);

const nodes = await locationAfter("/cluster/local/nodes");
assertEqual(nodes.pathname, "/cluster/local/nodes", "nodes path");
assertEqual(nodes.searchStr, "", "nodes has no query");
assertEqual(
  nodes.ids,
  ["__root__", "/cluster/$cluster", "/cluster/$cluster/nodes"],
  "nodes match ids",
);

const node = await locationAfter("/cluster/local/nodes/1");
assertEqual(node.pathname, "/cluster/local/nodes/1", "node detail path");
assertEqual(
  node.ids,
  ["__root__", "/cluster/$cluster", "/cluster/$cluster/nodes_/$id"],
  "node detail match ids",
);

const home = await locationAfter("/");
assertEqual(home.pathname, "/", "home path stays / until HomePage navigates");
assertEqual(home.ids, ["__root__", "/"], "home match ids");

const unknown = await locationAfter("/nope");
assertEqual(unknown.pathname, "/nope", "unmatched path stays in the URL");
assertEqual(unknown.ids, ["__root__"], "root 404 matches only the root route");

const clusterUnknown = await locationAfter("/cluster/local/does-not-exist");
assertEqual(
  clusterUnknown.pathname,
  "/cluster/local/does-not-exist",
  "unknown cluster child stays in the URL",
);
assertEqual(
  clusterUnknown.ids,
  ["__root__", "/cluster/$cluster"],
  "cluster 404 keeps the layout match",
);

console.log("router contract tests passed");
