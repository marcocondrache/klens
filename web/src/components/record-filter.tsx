import { useMemo, useState, type ReactNode } from "react";
import { CodeIcon, FilterIcon, PlusIcon, SearchIcon, XIcon } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ButtonGroup } from "@/components/ui/button-group";
import { Input } from "@/components/ui/input";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
  InputGroupTextarea,
} from "@/components/ui/input-group";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  FIELD_ITEMS,
  OPERATOR_ITEMS,
  type FilterClause,
  type FilterFieldKind,
  type FilterOperator,
  clauseSummary,
  defaultOperator,
  operatorsFor,
} from "@/lib/cel/builder";

export type FilterMode = "filters" | "cel";

const CEL_PLACEHOLDER = `value.status == "FAILED"
key.startsWith("ord_")
headers["x-trace-id"] == "abc"`;

export function RecordFilterControls({
  contains,
  onContainsChange,
  clauses,
  onClausesChange,
  mode,
  onModeChange,
  cel,
  onCelChange,
}: {
  contains: string;
  onContainsChange: (value: string) => void;
  clauses: FilterClause[];
  onClausesChange: (clauses: FilterClause[]) => void;
  mode: FilterMode;
  onModeChange: (mode: FilterMode) => void;
  cel: string;
  onCelChange: (value: string) => void;
}) {
  return (
    <div className="flex min-w-0 flex-1 flex-col gap-2">
      <div className="flex flex-wrap items-center gap-2">
        {mode === "filters" ? (
          <InputGroup className="w-full max-w-sm">
            <InputGroupAddon>
              <SearchIcon />
            </InputGroupAddon>
            <InputGroupInput
              value={contains}
              onChange={(event) => onContainsChange(event.target.value)}
              placeholder="Search key or value…"
            />
          </InputGroup>
        ) : null}

        <ButtonGroup className="ml-auto">
          <Button
            type="button"
            size="sm"
            variant={mode === "filters" ? "secondary" : "outline"}
            onClick={() => onModeChange("filters")}
            aria-pressed={mode === "filters"}
          >
            <FilterIcon />
            Filters
          </Button>
          <Button
            type="button"
            size="sm"
            variant={mode === "cel" ? "secondary" : "outline"}
            onClick={() => onModeChange("cel")}
            aria-pressed={mode === "cel"}
          >
            <CodeIcon />
            CEL
          </Button>
        </ButtonGroup>
      </div>

      {mode === "filters" ? (
        <div className="flex flex-wrap items-center gap-2">
          {clauses.map((item) => (
            <Badge
              key={item.id}
              variant="outline"
              className="h-7 gap-1 rounded-lg px-2 font-normal"
            >
              <span className="max-w-64 truncate font-mono text-xs">{clauseSummary(item)}</span>
              <button
                type="button"
                className="rounded-sm p-0.5 text-muted-foreground hover:text-foreground"
                aria-label={`Remove ${clauseSummary(item)}`}
                onClick={() => onClausesChange(clauses.filter((clause) => clause.id !== item.id))}
              >
                <XIcon className="size-3" />
              </button>
            </Badge>
          ))}
          <AddFilterButton onAdd={(clause) => onClausesChange([...clauses, clause])} />
        </div>
      ) : (
        <InputGroup className="h-auto min-h-16 w-full max-w-3xl">
          <InputGroupAddon align="block-start">
            <CodeIcon />
          </InputGroupAddon>
          <InputGroupTextarea
            value={cel}
            onChange={(event) => onCelChange(event.target.value)}
            placeholder={CEL_PLACEHOLDER}
            className="min-h-16 font-mono text-sm"
            aria-label="CEL filter"
          />
        </InputGroup>
      )}
    </div>
  );
}

function AddFilterButton({ onAdd }: { onAdd: (clause: FilterClause) => void }) {
  const [open, setOpen] = useState(false);
  const [field, setField] = useState<FilterFieldKind>("valuePath");
  const [path, setPath] = useState("");
  const [header, setHeader] = useState("");
  const [operator, setOperator] = useState<FilterOperator>("eq");
  const [value, setValue] = useState("");
  const operators = useMemo(() => operatorsFor(field), [field]);
  const operatorItems = OPERATOR_ITEMS.filter((item) => operators.includes(item.value));
  const needsValue = operator !== "exists";
  const extraLabel =
    field === "valuePath" ? "Field path" : field === "header" ? "Header name" : null;

  function reset(nextField: FilterFieldKind) {
    setField(nextField);
    setOperator(defaultOperator(nextField));
    setPath("");
    setHeader("");
    setValue("");
  }

  function add() {
    onAdd({
      id: crypto.randomUUID(),
      field,
      path,
      header,
      operator,
      value,
    });
    reset(field);
    setOpen(false);
  }

  const canAdd =
    (field !== "valuePath" || path.trim() !== "") &&
    (field !== "header" || header.trim() !== "") &&
    (!needsValue || value.trim() !== "");

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger render={<Button type="button" size="sm" variant="outline" />}>
        <PlusIcon />
        Filter
      </PopoverTrigger>
      <PopoverContent align="start" className="w-80">
        <div className="flex flex-col gap-3">
          <FieldControl label="Field">
            <Select
              value={field}
              items={FIELD_ITEMS}
              onValueChange={(next) => reset(String(next) as FilterFieldKind)}
            >
              <SelectTrigger size="sm" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {FIELD_ITEMS.map((item) => (
                  <SelectItem key={item.value} value={item.value}>
                    {item.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </FieldControl>

          {extraLabel ? (
            <FieldControl label={extraLabel}>
              <Input
                value={field === "valuePath" ? path : header}
                onChange={(event) =>
                  field === "valuePath"
                    ? setPath(event.target.value)
                    : setHeader(event.target.value)
                }
                placeholder={field === "valuePath" ? "user.id" : "x-trace-id"}
                className="h-8 font-mono text-sm"
              />
            </FieldControl>
          ) : null}

          <FieldControl label="Operator">
            <Select
              value={operator}
              items={operatorItems}
              onValueChange={(next) => setOperator(String(next) as FilterOperator)}
            >
              <SelectTrigger size="sm" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {operatorItems.map((item) => (
                  <SelectItem key={item.value} value={item.value}>
                    {item.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </FieldControl>

          {needsValue ? (
            <FieldControl label="Value">
              <Input
                value={value}
                onChange={(event) => setValue(event.target.value)}
                placeholder={field === "size" ? "1024" : "FAILED"}
                className="h-8 font-mono text-sm"
              />
            </FieldControl>
          ) : null}

          <Button type="button" size="sm" onClick={add} disabled={!canAdd}>
            Add filter
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  );
}

function FieldControl({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-1.5">
      <Label className="text-xs text-muted-foreground">{label}</Label>
      {children}
    </div>
  );
}
