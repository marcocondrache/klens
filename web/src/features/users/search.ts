import * as z from "zod/mini";

import type { ScramMechanism } from "@/lib/api/types";
import { filter, term } from "@/lib/route-search";

export const SCRAM_MECHANISMS = ["SHA256", "SHA512"] as const satisfies readonly ScramMechanism[];

export const usersSearch = z.object({
  q: term,
  mechanism: filter(SCRAM_MECHANISMS),
});

export type UsersSearch = z.output<typeof usersSearch>;
export type UserFilter = Exclude<keyof UsersSearch, "q">;
