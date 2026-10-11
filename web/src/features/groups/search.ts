import * as z from "zod/mini";

import type { GroupState } from "@/lib/api/types";
import { filter, term } from "@/lib/route-search";

export const GROUP_STATES = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "ASSIGNING",
  "RECONCILING",
  "DEAD",
  "UNKNOWN",
] as const satisfies readonly GroupState[];

const GROUP_LAG = ["lagging", "caught-up"] as const;

export const groupsSearch = z.object({
  q: term,
  state: filter(GROUP_STATES),
  lag: filter(GROUP_LAG),
});

export type GroupsSearch = z.output<typeof groupsSearch>;
export type GroupFilter = Exclude<keyof GroupsSearch, "q">;

const groupTabParam = z.catch(z._default(z.enum(["offsets", "members"]), "offsets"), "offsets");

export const groupDetailSearch = z.object({
  tab: groupTabParam,
});

export function groupTab(value: unknown) {
  return z.parse(groupTabParam, value);
}
