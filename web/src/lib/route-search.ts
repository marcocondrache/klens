import * as z from "zod/mini";

import { filterParam } from "@/components/data-table/filters";

export const term = z.catch(z._default(z.string(), ""), "");

export const flag = z.catch(
  z._default(
    z.union([
      z.boolean(),
      z.pipe(
        z.enum(["true", "false"]),
        z.transform((value) => value === "true"),
      ),
    ]),
    false,
  ),
  false,
);

export function oneOf<const T extends readonly [string, ...string[]]>(values: T) {
  return z.catch(z.optional(z.enum(values)), undefined);
}

export function filter(allowed: readonly string[]) {
  const param = z.pipe(
    z.string(),
    z.transform((raw) => filterParam(allowed, raw)),
  );
  return z.catch(z.optional(param), undefined);
}

export function searchDefaults<T extends z.ZodMiniType>(schema: T): z.output<T> {
  return z.parse(schema, {});
}

export function parseSearch(searchStr: string): Record<string, string> {
  const query = searchStr.startsWith("?") ? searchStr.slice(1) : searchStr;
  const params = new URLSearchParams(query);
  const out: Record<string, string> = {};
  params.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}

export function stringifySearch(search: Record<string, unknown>): string {
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
