import * as z from "zod/mini";

import { term } from "@/lib/route-search";

const version = z.catch(
  z.optional(
    z.union([
      z.int().check(z.positive()),
      z.pipe(
        z.string().check(z.regex(/^[1-9]\d*$/)),
        z.transform((value) => Number(value)),
      ),
    ]),
  ),
  undefined,
);

export const schemasSearch = z.object({
  q: term,
  subject: z.catch(z.optional(z.string()), undefined),
  version,
});
