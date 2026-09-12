export type TopicsSearch = {
  q?: string;
  internal?: "1";
  policy?: "delete" | "compact";
};

export type GroupsSearch = {
  q?: string;
  state?: "STABLE" | "EMPTY" | "PREPARING_REBALANCE" | "COMPLETING_REBALANCE" | "DEAD";
};

export type SchemasSearch = {
  q?: string;
};

export type LoginSearch = {
  error?: string;
};

export type TopicDetailSearch = {
  tab?: "partitions" | "groups" | "config";
};

export type GroupDetailSearch = {
  tab?: "members";
};

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

export function parseTopicsSearch(search: Record<string, unknown>): TopicsSearch {
  const q = optionalString(search.q);
  const policy =
    search.policy === "delete" || search.policy === "compact" ? search.policy : undefined;
  return {
    ...(q ? { q } : {}),
    ...(search.internal === "1" ? { internal: "1" as const } : {}),
    ...(policy ? { policy } : {}),
  };
}

export function parseGroupsSearch(search: Record<string, unknown>): GroupsSearch {
  const q = optionalString(search.q);
  const state =
    search.state === "STABLE" ||
    search.state === "EMPTY" ||
    search.state === "PREPARING_REBALANCE" ||
    search.state === "COMPLETING_REBALANCE" ||
    search.state === "DEAD"
      ? search.state
      : undefined;
  return {
    ...(q ? { q } : {}),
    ...(state ? { state } : {}),
  };
}

export function parseSchemasSearch(search: Record<string, unknown>): SchemasSearch {
  const q = optionalString(search.q);
  return q ? { q } : {};
}

export function parseLoginSearch(search: Record<string, unknown>): LoginSearch {
  const error = optionalString(search.error);
  return error ? { error } : {};
}

export function parseTopicDetailSearch(search: Record<string, unknown>): TopicDetailSearch {
  const tab = search.tab;
  if (tab === "partitions" || tab === "groups" || tab === "config") {
    return { tab };
  }
  return {};
}

export function parseGroupDetailSearch(search: Record<string, unknown>): GroupDetailSearch {
  return search.tab === "members" ? { tab: "members" } : {};
}
