export type FilterFieldKind =
  | "keyText"
  | "valueText"
  | "valuePath"
  | "header"
  | "schemaId"
  | "size";

export type FilterOperator =
  | "eq"
  | "neq"
  | "contains"
  | "startsWith"
  | "endsWith"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "exists";

export type FilterClause = {
  id: string;
  field: FilterFieldKind;
  path: string;
  header: string;
  operator: FilterOperator;
  value: string;
};

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

export const FIELD_ITEMS: { value: FilterFieldKind; label: string }[] = [
  { value: "keyText", label: "Key text" },
  { value: "valueText", label: "Value text" },
  { value: "valuePath", label: "Value field" },
  { value: "header", label: "Header" },
  { value: "schemaId", label: "Schema ID" },
  { value: "size", label: "Size" },
];

export const OPERATOR_ITEMS: { value: FilterOperator; label: string }[] = [
  { value: "eq", label: "equals" },
  { value: "neq", label: "not equals" },
  { value: "contains", label: "contains" },
  { value: "startsWith", label: "starts with" },
  { value: "endsWith", label: "ends with" },
  { value: "gt", label: "greater than" },
  { value: "gte", label: "at least" },
  { value: "lt", label: "less than" },
  { value: "lte", label: "at most" },
  { value: "exists", label: "exists" },
];

const OPERATORS_BY_FIELD: Record<FilterFieldKind, FilterOperator[]> = {
  keyText: ["eq", "neq", "contains", "startsWith", "endsWith"],
  valueText: ["eq", "neq", "contains", "startsWith", "endsWith"],
  valuePath: [
    "eq",
    "neq",
    "contains",
    "startsWith",
    "endsWith",
    "gt",
    "gte",
    "lt",
    "lte",
    "exists",
  ],
  header: ["eq", "neq", "contains", "startsWith", "endsWith", "exists"],
  schemaId: ["eq", "neq", "gt", "gte", "lt", "lte"],
  size: ["eq", "neq", "gt", "gte", "lt", "lte"],
};

export function operatorsFor(field: FilterFieldKind): FilterOperator[] {
  return OPERATORS_BY_FIELD[field];
}

export function defaultOperator(field: FilterFieldKind): FilterOperator {
  const operators = operatorsFor(field);
  return operators.includes("contains") ? "contains" : "eq";
}

export function operatorLabel(operator: FilterOperator): string {
  return OPERATOR_ITEMS.find((item) => item.value === operator)?.label ?? operator;
}

export function clauseSummary(clause: FilterClause): string {
  const target = clauseTarget(clause);
  if (clause.operator === "exists") {
    return `${target} exists`;
  }
  return `${target} ${operatorLabel(clause.operator)} ${clause.value || "…"}`;
}

export function builderToCel(input: { contains: string; clauses: FilterClause[] }): string {
  const parts: string[] = [];
  const contains = input.contains.trim();
  if (contains) {
    const needle = celString(contains);
    parts.push(
      `(keyText.lowerAscii().contains(${needle}) || valueText.lowerAscii().contains(${needle}))`,
    );
  }

  for (const clause of input.clauses) {
    const expr = clauseToCel(clause);
    if (expr) parts.push(expr);
  }

  return parts.join(" && ");
}

export function celString(value: string): string {
  return `"${value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\t", "\\t")}"`;
}

function clauseToCel(clause: FilterClause): string | null {
  const left = clauseLeft(clause);
  if (!left) return null;

  if (clause.operator === "exists") {
    return existsExpr(clause, left);
  }

  const raw = clause.value;
  if (raw === "" && needsValue(clause.operator)) {
    return null;
  }

  const text = stringExpr(left, clause.field);
  switch (clause.operator) {
    case "eq":
      return `${left} == ${celLiteral(raw, clause.field)}`;
    case "neq":
      return `${left} != ${celLiteral(raw, clause.field)}`;
    case "contains":
      return `${text}.lowerAscii().contains(${celString(raw)})`;
    case "startsWith":
      return `${text}.startsWith(${celString(raw)})`;
    case "endsWith":
      return `${text}.endsWith(${celString(raw)})`;
    case "gt":
      return `${left} > ${celLiteral(raw, clause.field)}`;
    case "gte":
      return `${left} >= ${celLiteral(raw, clause.field)}`;
    case "lt":
      return `${left} < ${celLiteral(raw, clause.field)}`;
    case "lte":
      return `${left} <= ${celLiteral(raw, clause.field)}`;
  }
}

function clauseLeft(clause: FilterClause): string | null {
  switch (clause.field) {
    case "keyText":
      return "keyText";
    case "valueText":
      return "valueText";
    case "valuePath":
      return valuePathExpr(clause.path);
    case "header":
      return headerExpr(clause.header);
    case "schemaId":
      return "schemaId";
    case "size":
      return "size";
  }
}

function valuePathExpr(path: string): string | null {
  const parts = path
    .split(".")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length === 0) return null;
  if (parts.every((part) => IDENT.test(part))) {
    return `value.${parts.join(".")}`;
  }
  return parts.reduce((expr, part) => `${expr}[${celString(part)}]`, "value");
}

function headerExpr(name: string): string | null {
  const trimmed = name.trim();
  if (!trimmed) return null;
  return `headers[${celString(trimmed)}]`;
}

function existsExpr(clause: FilterClause, left: string): string {
  if (clause.field === "header") {
    return `${celString(clause.header.trim())} in headers`;
  }
  if (clause.field === "valuePath") {
    return `has(${left})`;
  }
  return `has(${left})`;
}

function stringExpr(left: string, field: FilterFieldKind): string {
  if (field === "keyText" || field === "valueText" || field === "header") {
    return left;
  }
  return `string(${left})`;
}

function celLiteral(value: string, field: FilterFieldKind): string {
  if (field === "schemaId" || field === "size") {
    return numericLiteral(value);
  }
  const trimmed = value.trim();
  if (trimmed === "true" || trimmed === "false" || trimmed === "null") {
    return trimmed;
  }
  if (/^-?\d+$/.test(trimmed)) {
    return trimmed;
  }
  if (/^-?\d+\.\d+$/.test(trimmed)) {
    return trimmed;
  }
  return celString(value);
}

function numericLiteral(value: string): string {
  const trimmed = value.trim();
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return trimmed;
  }
  return "0";
}

function needsValue(operator: FilterOperator): boolean {
  return operator !== "exists";
}

function clauseTarget(clause: FilterClause): string {
  switch (clause.field) {
    case "keyText":
      return "key";
    case "valueText":
      return "value";
    case "valuePath":
      return clause.path.trim() ? `value.${clause.path.trim()}` : "value field";
    case "header":
      return clause.header.trim() ? `header ${clause.header.trim()}` : "header";
    case "schemaId":
      return "schema ID";
    case "size":
      return "size";
  }
}
