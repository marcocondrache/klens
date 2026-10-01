import * as z from "zod/mini";

import type { QuotaEntityType } from "@/lib/api/types";
import { filter, term } from "@/lib/route-search";

export const QUOTA_ENTITY_TYPES = [
  "USER",
  "CLIENT_ID",
  "IP",
] as const satisfies readonly QuotaEntityType[];
const QUOTA_SCOPES = ["named", "default"] as const;

export const quotasSearch = z.object({
  q: term,
  entity: filter(QUOTA_ENTITY_TYPES),
  scope: filter(QUOTA_SCOPES),
});

export type QuotasSearch = z.output<typeof quotasSearch>;
export type QuotaFilter = Exclude<keyof QuotasSearch, "q">;
