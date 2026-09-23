import type { ReactNode } from "react";
import { ChevronDownIcon, ListFilterIcon, XIcon, type LucideIcon } from "lucide-react";

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
  rows?: TData[];
  value: FilterRule[];
  onChange: (rules: FilterRule[]) => void;
  custom?: readonly CustomFilter[];
}

export interface CustomFilter {
  id: string;
  label: string;
  icon: LucideIcon;
  menu: ReactNode;
  chip: ReactNode;
  onClear: () => void;
}

export function FilterBar<TData>(props: FilterBarProps<TData>) {
  const { fields, value, onChange, custom = [] } = props;
  const active = custom.filter((filter) => filter.chip != null);
  const count = value.length + active.length;

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger
          render={
            <Button
              variant="outline"
              size={count > 0 ? "icon" : "default"}
              aria-label="Add filter"
              className="font-normal text-muted-foreground hover:text-foreground aria-expanded:text-foreground"
            />
          }
        >
          <ListFilterIcon className="size-3.5" />
          {count > 0 ? null : "Filter"}
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
          {custom.map((filter) => (
            <DropdownMenuSub key={filter.id}>
              <DropdownMenuSubTrigger>{filter.label}</DropdownMenuSubTrigger>
              <DropdownMenuSubContent className="min-w-40">{filter.menu}</DropdownMenuSubContent>
            </DropdownMenuSub>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>

      {value.map((rule) => {
        const field = fields.find((candidate) => candidate.id === rule.id);
        return field ? <FilterChip key={rule.id} field={field} rule={rule} {...props} /> : null;
      })}

      {active.map((filter) => (
        <ChipShell
          key={filter.id}
          label={filter.label}
          icon={filter.icon}
          onRemove={filter.onClear}
        >
          {filter.chip}
        </ChipShell>
      ))}

      {count > 0 ? (
        <Button
          variant="ghost"
          className="text-muted-foreground"
          onClick={() => {
            onChange([]);
            for (const filter of active) filter.onClear();
          }}
        >
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
  const counts = rows ? facetCounts(rows, fields, value, field) : null;

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
      <DropdownMenuShortcut>
        {counts ? (counts.get(option.value) ?? 0) : option.hint}
      </DropdownMenuShortcut>
    </DropdownMenuCheckboxItem>
  ));
}

export const CHIP_SEGMENT =
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
    <ChipShell
      label={field.label}
      icon={field.icon}
      onRemove={() => onChange(value.filter((existing) => existing !== rule))}
    >
      <DropdownMenu>
        <DropdownMenuTrigger className={cn(CHIP_SEGMENT, "text-muted-foreground")}>
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
          className={cn(CHIP_SEGMENT, "font-medium")}
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
    </ChipShell>
  );
}

function ChipShell({
  label,
  icon: Icon,
  onRemove,
  children,
}: {
  label: string;
  icon: LucideIcon;
  onRemove: () => void;
  children: ReactNode;
}) {
  return (
    <div className="flex h-8 items-center divide-x overflow-hidden rounded-lg border bg-background text-sm duration-150 animate-in fade-in-0 zoom-in-95 motion-reduce:animate-none dark:divide-input dark:border-input dark:bg-input/20">
      <span className="flex h-full items-center gap-1.5 px-2.5 text-muted-foreground">
        <Icon className="size-3.5 shrink-0" />
        {label}
      </span>

      {children}

      <button
        type="button"
        aria-label={`Remove ${label.toLowerCase()} filter`}
        className={cn(CHIP_SEGMENT, "text-muted-foreground")}
        onClick={onRemove}
      >
        <XIcon className="size-3.5" />
      </button>
    </div>
  );
}
