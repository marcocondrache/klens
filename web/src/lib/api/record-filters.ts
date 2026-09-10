import {
  BracesIcon,
  ClockIcon,
  CodeIcon,
  HashIcon,
  KeyIcon,
  LayersIcon,
  ScaleIcon,
  TagIcon,
  Trash2Icon,
} from "lucide-react";

import { resolveDateRange } from "@/lib/filters/date-range";
import { isEmptyFilterValue } from "@/lib/filters/state";
import type { FilterFieldDef, FilterState, NumberOp } from "@/lib/filters/types";
import type { Topic } from "./types";

const CEL_OPS: Record<NumberOp, string> = {
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
  eq: "==",
};

/** Filters the record browser offers, derived from the topic being browsed. */
export function recordFilterFields(topic: Topic): FilterFieldDef[] {
  return [
    {
      id: "time",
      label: "Time",
      icon: ClockIcon,
      kind: "date-range",
      keywords: "timestamp when date range",
    },
    {
      id: "partition",
      label: "Partition",
      icon: LayersIcon,
      kind: "multi-select",
      anyLabel: "All partitions",
      options: topic.partitions.map((partition) => ({
        value: String(partition.id),
        label: `Partition ${partition.id}`,
        keywords: String(partition.id),
      })),
    },
    {
      id: "key",
      label: "Key",
      icon: KeyIcon,
      kind: "text",
      mono: true,
      placeholder: "Key contains…",
    },
    {
      id: "value",
      label: "Value",
      icon: BracesIcon,
      kind: "text",
      mono: true,
      placeholder: "Value contains…",
      keywords: "payload body",
    },
    {
      id: "header",
      label: "Header",
      icon: TagIcon,
      kind: "key-value",
      keyPlaceholder: "Header key",
      valuePlaceholder: "Value contains… (optional)",
    },
    {
      id: "size",
      label: "Size",
      icon: ScaleIcon,
      kind: "number",
      unit: "bytes",
      placeholder: "Bytes",
      ops: ["gt", "gte", "lt", "lte"],
    },
    {
      id: "offset",
      label: "Offset",
      icon: HashIcon,
      kind: "number-range",
      fromPlaceholder: "From offset",
      toPlaceholder: "To offset",
    },
    {
      id: "tombstone",
      label: "Tombstones",
      icon: Trash2Icon,
      kind: "choice",
      anyLabel: "Any record",
      keywords: "null deleted compacted",
      options: [
        { value: "only", label: "Only tombstones" },
        { value: "exclude", label: "Exclude tombstones" },
      ],
    },
    {
      id: "cel",
      label: "Advanced (CEL)",
      icon: CodeIcon,
      kind: "raw",
      keywords: "expression cel query",
      placeholder: 'value.status == "FAILED"',
      hint: "CEL over key, value, headers, partition, offset, timestamp, size and schemaId.",
    },
  ];
}

export type CompiledRecordFilters = {
  /** CEL expression for the backend `filter` field. */
  filter: string | null;
  timestampFrom: string | null;
  timestampTo: string | null;
  partition: number | null;
};

function quote(value: string) {
  return JSON.stringify(value);
}

function contains(variable: "keyText" | "valueText", needle: string) {
  return `${variable}.lowerAscii().contains(${quote(needle.trim().toLowerCase())})`;
}

/** Compile the quick search box into a key or value substring match. */
export function searchClause(term: string): string | null {
  if (!term.trim()) return null;
  return `${contains("keyText", term)} || ${contains("valueText", term)}`;
}

/**
 * Turn filter state into record query arguments. Time and a single selected
 * partition become native query fields because they narrow the offset window
 * the backend scans; everything else becomes a CEL clause.
 */
export function compileRecordFilters(
  state: FilterState,
  options: { search?: string } = {},
): CompiledRecordFilters {
  const clauses: string[] = [];
  let timestampFrom: string | null = null;
  let timestampTo: string | null = null;
  let partition: number | null = null;

  const search = searchClause(options.search ?? "");
  if (search) clauses.push(search);

  for (const [id, value] of Object.entries(state)) {
    if (isEmptyFilterValue(value)) continue;

    switch (id) {
      case "time": {
        if (value.kind !== "date-range") break;
        const range = resolveDateRange(value);
        timestampFrom = range.from;
        timestampTo = range.to;
        break;
      }
      case "partition": {
        if (value.kind !== "multi-select") break;
        const ids = value.values.map(Number).filter((entry) => Number.isInteger(entry));
        if (ids.length === 1) {
          partition = ids[0] ?? null;
        } else if (ids.length > 1) {
          clauses.push(ids.map((entry) => `partition == ${entry}`).join(" || "));
        }
        break;
      }
      case "key": {
        if (value.kind !== "text") break;
        clauses.push(contains("keyText", value.text));
        break;
      }
      case "value": {
        if (value.kind !== "text") break;
        clauses.push(contains("valueText", value.text));
        break;
      }
      case "header": {
        if (value.kind !== "key-value") break;
        const key = quote(value.key.trim());
        clauses.push(
          value.value.trim()
            ? `headers[${key}].lowerAscii().contains(${quote(value.value.trim().toLowerCase())})`
            : `${key} in headers`,
        );
        break;
      }
      case "size": {
        if (value.kind !== "number" || value.value == null) break;
        clauses.push(`size ${CEL_OPS[value.op]} ${value.value}`);
        break;
      }
      case "offset": {
        if (value.kind !== "number-range") break;
        if (value.from != null) clauses.push(`offset >= ${value.from}`);
        if (value.to != null) clauses.push(`offset <= ${value.to}`);
        break;
      }
      case "tombstone": {
        if (value.kind !== "choice") break;
        clauses.push(value.value === "only" ? "value == null" : "value != null");
        break;
      }
      case "cel": {
        if (value.kind !== "raw") break;
        clauses.push(value.expression.trim());
        break;
      }
    }
  }

  return {
    filter: clauses.length === 0 ? null : clauses.map((clause) => `(${clause})`).join(" && "),
    timestampFrom,
    timestampTo,
    partition,
  };
}
