import * as z from "zod/mini";

import { filter, flag, oneOf, term } from "@/lib/route-search";

const TOPIC_POLICIES = ["delete", "compact"] as const;
const TOPIC_HEALTH = ["under-replicated", "in-sync"] as const;
const TOPIC_ACTIVITY = ["active", "idle"] as const;

export const topicsSearch = z.object({
  q: term,
  internal: flag,
  policy: filter(TOPIC_POLICIES),
  health: filter(TOPIC_HEALTH),
  activity: filter(TOPIC_ACTIVITY),
});

export type TopicsSearch = z.output<typeof topicsSearch>;
export type TopicFilter = Exclude<keyof TopicsSearch, "q" | "internal">;

const topicTabParam = oneOf(["partitions", "groups", "config"]);

const position = z.catch(
  z.optional(
    z.union([
      z.int().check(z.nonnegative()),
      z.pipe(
        z.string().check(z.regex(/^\d+$/)),
        z.transform((value) => Number(value)),
      ),
    ]),
  ),
  undefined,
);

export const topicDetailSearch = z.object({
  tab: topicTabParam,
  partition: position,
  offset: position,
});

export function topicTab(value: unknown) {
  return z.parse(topicTabParam, value);
}
