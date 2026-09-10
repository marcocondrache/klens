import {
  builderToCel,
  celString,
  clauseSummary,
  type FilterClause,
} from "../src/lib/cel/builder.ts";

function clause(
  partial: Partial<FilterClause> & Pick<FilterClause, "field" | "operator">,
): FilterClause {
  return {
    id: partial.id ?? "1",
    path: "",
    header: "",
    value: "",
    ...partial,
  };
}

function assertEqual(actual: unknown, expected: unknown) {
  if (actual !== expected) {
    throw new Error(`expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

assertEqual(builderToCel({ contains: "  ", clauses: [] }), "");

assertEqual(
  builderToCel({ contains: "Refund", clauses: [] }),
  '(keyText.lowerAscii().contains("Refund") || valueText.lowerAscii().contains("Refund"))',
);

assertEqual(
  builderToCel({ contains: `a"b\\c`, clauses: [] }),
  '(keyText.lowerAscii().contains("a\\"b\\\\c") || valueText.lowerAscii().contains("a\\"b\\\\c"))',
);
assertEqual(celString(`say "hi"`), `"say \\"hi\\""`);

assertEqual(
  builderToCel({
    contains: "ord_",
    clauses: [
      clause({
        field: "valuePath",
        path: "status",
        operator: "eq",
        value: "FAILED",
      }),
    ],
  }),
  '(keyText.lowerAscii().contains("ord_") || valueText.lowerAscii().contains("ord_")) && value.status == "FAILED"',
);

assertEqual(
  builderToCel({
    contains: "",
    clauses: [
      clause({ field: "valuePath", path: "user.id", operator: "eq", value: "42" }),
      clause({ field: "header", header: "x-trace-id", operator: "eq", value: "abc" }),
      clause({ field: "size", operator: "gt", value: "1024" }),
      clause({ field: "schemaId", operator: "eq", value: "7" }),
      clause({ field: "valuePath", path: "user.email", operator: "exists" }),
      clause({ field: "header", header: "x-trace-id", operator: "exists" }),
      clause({
        field: "valuePath",
        path: "tags",
        operator: "contains",
        value: "urgent",
      }),
      clause({ field: "keyText", operator: "startsWith", value: "ord_" }),
    ],
  }),
  [
    "value.user.id == 42",
    'headers["x-trace-id"] == "abc"',
    "size > 1024",
    "schemaId == 7",
    "has(value.user.email)",
    '"x-trace-id" in headers',
    'string(value.tags).lowerAscii().contains("urgent")',
    'keyText.startsWith("ord_")',
  ].join(" && "),
);

assertEqual(
  builderToCel({
    contains: "",
    clauses: [
      clause({
        field: "valuePath",
        path: "user-name.full name",
        operator: "eq",
        value: "Ada",
      }),
    ],
  }),
  'value["user-name"]["full name"] == "Ada"',
);

assertEqual(
  builderToCel({
    contains: "",
    clauses: [
      clause({ field: "valuePath", path: "", operator: "eq", value: "x" }),
      clause({ field: "header", header: "", operator: "eq", value: "x" }),
      clause({ field: "keyText", operator: "contains", value: "" }),
    ],
  }),
  "",
);

assertEqual(
  clauseSummary(clause({ field: "valuePath", path: "status", operator: "eq", value: "FAILED" })),
  "value.status equals FAILED",
);
assertEqual(
  clauseSummary(clause({ field: "header", header: "trace", operator: "exists" })),
  "header trace exists",
);

console.log("builderToCel tests passed");
