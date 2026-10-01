import { useMemo } from "react";

import { useSearchDraft } from "@/hooks/use-search-draft";

import {
  applyFilters,
  filterParams,
  readFilters,
  type FilterField,
  type FilterRule,
} from "./filters";

type TableSearch<TId extends string> = { q: string } & Partial<Record<TId, string>>;

type TableSearchPatch<TId extends string> = { q: string } | Record<TId, string | undefined>;

export function useTableSearch<TData, TId extends string>({
  rows,
  fields,
  search,
  setSearch,
  matches,
}: {
  rows: TData[];
  fields: ReadonlyArray<FilterField<TData, TId>>;
  search: TableSearch<TId>;
  setSearch: (patch: TableSearchPatch<TId>) => void;
  matches: (row: TData, needle: string) => boolean;
}) {
  const searchInput = useSearchDraft(search.q, (q) => setSearch({ q }));
  const filters = readFilters(fields, search);
  const needle = search.q.trim().toLowerCase();

  const searched = useMemo(
    () => (needle ? rows.filter((row) => matches(row, needle)) : rows),
    [rows, needle, matches],
  );

  return {
    searchInput,
    rows: applyFilters(searched, fields, filters),
    filterBar: {
      fields,
      rows: searched,
      value: filters,
      onChange: (rules: FilterRule[]) => setSearch(filterParams(fields, rules)),
    },
  };
}
