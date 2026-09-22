import { ChevronDownIcon, ListFilterIcon, XIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuShortcut,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";

import {
  facetCounts,
  operatorLabel,
  setFilterValues,
  type FilterField,
  type FilterRule,
} from "./filters";

interface FilterBarProps<TData> {
  fields: ReadonlyArray<FilterField<TData>>;
  /** Rows before these filters apply; used for option counts. */
  rows: TData[];
  value: FilterRule[];
  onChange: (rules: FilterRule[]) => void;
}

export function FilterBar<TData>(props: FilterBarProps<TData>) {
  const { fields, value, onChange } = props;

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger
          render={
            <Button
              variant="outline"
              size={value.length > 0 ? "icon" : "default"}
              aria-label="Add filter"
            />
          }
        >
          <ListFilterIcon />
          {value.length > 0 ? null : "Filter"}
        </DropdownMenuTrigger>
        <DropdownMenuContent className="min-w-40">
          {fields.map((field) => (
            <DropdownMenuSub key={field.id}>
              <DropdownMenuSubTrigger>{field.label}</DropdownMenuSubTrigger>
              <DropdownMenuSubContent className="min-w-40">
                <OptionItems field={field} {...props} />
              </DropdownMenuSubContent>
            </DropdownMenuSub>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>

      {value.map((rule) => {
        const field = fields.find((candidate) => candidate.id === rule.id);
        return field ? <FilterChip key={rule.id} field={field} rule={rule} {...props} /> : null;
      })}

      {value.length > 0 ? (
        <Button variant="ghost" className="text-muted-foreground" onClick={() => onChange([])}>
          Clear
        </Button>
      ) : null}
    </>
  );
}

/** Checkbox items for a field's options, with how many rows each would match. */
function OptionItems<TData>({
  field,
  fields,
  rows,
  value,
  onChange,
}: FilterBarProps<TData> & { field: FilterField<TData> }) {
  const selected = value.find((rule) => rule.id === field.id)?.values ?? [];
  const counts = facetCounts(rows, fields, value, field);

  function toggle(option: string, checked: boolean) {
    const next = checked
      ? [...selected, option]
      : selected.filter((existing) => existing !== option);
    onChange(setFilterValues(value, field.id, next));
  }

  return field.options.map((option) => (
    <DropdownMenuCheckboxItem
      key={option.value}
      checked={selected.includes(option.value)}
      onCheckedChange={(checked) => toggle(option.value, checked)}
    >
      {option.label}
      <DropdownMenuShortcut>{counts.get(option.value) ?? 0}</DropdownMenuShortcut>
    </DropdownMenuCheckboxItem>
  ));
}

const SEGMENT =
  "flex h-full items-center gap-1.5 px-2 outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:bg-muted focus-visible:text-foreground aria-expanded:bg-muted aria-expanded:text-foreground";

function FilterChip<TData>({
  field,
  rule,
  ...props
}: FilterBarProps<TData> & { field: FilterField<TData>; rule: FilterRule }) {
  const { value, onChange } = props;
  const selected = field.options.filter((option) => rule.values.includes(option.value));

  function setNegate(negate: boolean) {
    onChange(value.map((existing) => (existing === rule ? { ...existing, negate } : existing)));
  }

  return (
    <div className="flex h-8 items-center divide-x overflow-hidden rounded-lg border bg-background text-sm duration-150 animate-in fade-in-0 zoom-in-95 motion-reduce:animate-none dark:divide-input dark:border-input dark:bg-input/30">
      <span className="flex h-full items-center gap-1.5 px-2.5 text-muted-foreground">
        <field.icon className="size-4 shrink-0" />
        {field.label}
      </span>

      <DropdownMenu>
        <DropdownMenuTrigger className={cn(SEGMENT, "text-muted-foreground")}>
          {operatorLabel(rule)}
        </DropdownMenuTrigger>
        <DropdownMenuContent className="w-auto">
          {[false, true].map((negate) => (
            <DropdownMenuItem key={String(negate)} onClick={() => setNegate(negate)}>
              {operatorLabel(rule, negate)}
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>

      <DropdownMenu>
        <DropdownMenuTrigger
          aria-label={`${field.label} values`}
          className={cn(SEGMENT, "font-medium")}
        >
          {selected.length === 1 ? (
            <>
              {selected[0].icon}
              <span className="max-w-40 truncate">{selected[0].label}</span>
            </>
          ) : (
            <>
              {selected.some((option) => option.icon) ? (
                <span className="flex -space-x-1">
                  {selected.slice(0, 3).map((option) => (
                    <span key={option.value} className="flex">
                      {option.icon}
                    </span>
                  ))}
                </span>
              ) : null}
              {selected.length} {field.plural}
            </>
          )}
          <ChevronDownIcon className="size-3.5 text-muted-foreground" />
        </DropdownMenuTrigger>
        <DropdownMenuContent className="w-auto min-w-40">
          <OptionItems field={field} {...props} />
        </DropdownMenuContent>
      </DropdownMenu>

      <button
        type="button"
        aria-label={`Remove ${field.label.toLowerCase()} filter`}
        className={cn(SEGMENT, "text-muted-foreground")}
        onClick={() => onChange(value.filter((existing) => existing !== rule))}
      >
        <XIcon className="size-3.5" />
      </button>
    </div>
  );
}
