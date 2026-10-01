import * as z from "zod/mini";

import { oneOf } from "@/lib/route-search";

export const loginSearch = z.object({
  error: z.catch(z.optional(z.string()), undefined),
  from: oneOf(["callback"]),
});

export type LoginSearch = z.output<typeof loginSearch>;
