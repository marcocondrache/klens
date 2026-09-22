import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { createParser } from "nuqs";

export interface FilterOption {
  value: string;
  label: string;
  icon?: ReactNode;
}

export interface FilterField<TData> {
  /** Doubles as the URL search key. */
  id: string;
  label: string;
  plural: string;
  icon: LucideIcon;
  options: readonly FilterOption[];
  accessor: (row: TData) => string | readonly string[];
}

export interface FilterRule {
  id: string;
  values: string[];
  negate: boolean;
}

export function operatorLabel(rule: FilterRule, negate = rule.negate) {
  if (rule.values.length > 1) return negate ? "is none of" : "is any of";
  return negate ? "is not" : "is";
}

function rowMatches<TData>(row: TData, field: FilterField<TData>, rule: FilterRule) {
  const raw = field.accessor(row);
  const values: readonly string[] = typeof raw === "string" ? [raw] : raw;
  const hit = values.some((value) => rule.values.includes(value));
  return rule.negate ? !hit : hit;
}

export function applyFilters<TData>(
  rows: TData[],
  fields: ReadonlyArray<FilterField<TData>>,
  rules: FilterRule[],
  skip?: string,
) {
  const active = rules.flatMap((rule) => {
    const field = fields.find((candidate) => candidate.id === rule.id);
    return field && rule.id !== skip ? [{ field, rule }] : [];
  });
  if (active.length === 0) return rows;

  return rows.filter((row) => active.every(({ field, rule }) => rowMatches(row, field, rule)));
}

export function toggleFilterValue(rules: FilterRule[], id: string, value: string): FilterRule[] {
  const existing = rules.find((rule) => rule.id === id);
  if (!existing) return [...rules, { id, values: [value], negate: false }];

  const values = existing.values.includes(value)
    ? existing.values.filter((candidate) => candidate !== value)
    : [...existing.values, value];
  if (values.length === 0) return rules.filter((rule) => rule !== existing);
  return rules.map((rule) => (rule === existing ? { ...rule, values } : rule));
}

/** Option counts for `field`, honouring every other active filter. */
export function facetCounts<TData>(
  rows: TData[],
  fields: ReadonlyArray<FilterField<TData>>,
  rules: FilterRule[],
  field: FilterField<TData>,
) {
  const counts = new Map<string, number>();
  for (const row of applyFilters(rows, fields, rules, field.id)) {
    const raw = field.accessor(row);
    for (const value of typeof raw === "string" ? [raw] : raw) {
      counts.set(value, (counts.get(value) ?? 0) + 1);
    }
  }
  return counts;
}

export type FilterParam = Omit<FilterRule, "id">;

/** A `a,b` (any of) or `!a,b` (none of) search param, limited to `allowed`. */
export function parseAsFilter(allowed: readonly string[]) {
  return createParser<FilterParam>({
    parse(raw) {
      const negate = raw.startsWith("!");
      const values = [...new Set((negate ? raw.slice(1) : raw).split(","))].filter((value) =>
        allowed.includes(value),
      );
      return values.length > 0 ? { values, negate } : null;
    },
    serialize: ({ values, negate }) => `${negate ? "!" : ""}${values.join(",")}`,
    eq: (a, b) => a.negate === b.negate && a.values.join(",") === b.values.join(","),
  });
}

export function readFilters<TData>(
  fields: ReadonlyArray<FilterField<TData>>,
  search: Partial<Record<string, unknown>>,
): FilterRule[] {
  return fields.flatMap((field) => {
    const param = search[field.id] as FilterParam | null | undefined;
    return param ? [{ id: field.id, ...param }] : [];
  });
}

/** The query-state update that replaces every field's param with `rules`. */
export function filterParams<TData>(
  fields: ReadonlyArray<FilterField<TData>>,
  rules: FilterRule[],
): Record<string, FilterParam | null> {
  return Object.fromEntries(
    fields.map((field) => {
      const rule = rules.find((candidate) => candidate.id === field.id);
      return [field.id, rule ? { values: rule.values, negate: rule.negate } : null];
    }),
  );
}
