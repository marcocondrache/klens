import * as z from "zod/mini";

import type { ScramMechanism } from "@/lib/api/types";
import { filter, term } from "@/lib/route-search";

export const SCRAM_MECHANISMS = ["SHA256", "SHA512"] as const satisfies readonly ScramMechanism[];

export const MECHANISM_LABEL: Record<ScramMechanism, string> = {
  SHA256: "SCRAM-SHA-256",
  SHA512: "SCRAM-SHA-512",
};

export const usersSearch = z.object({
  q: term,
  mechanism: filter(SCRAM_MECHANISMS),
});

export type UsersSearch = z.output<typeof usersSearch>;
export type UserFilter = Exclude<keyof UsersSearch, "q">;
