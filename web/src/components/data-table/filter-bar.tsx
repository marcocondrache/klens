import { useState, type KeyboardEvent } from "react";
import { Combobox as ComboboxPrimitive } from "@base-ui/react";
import { ListFilterIcon, XIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Combobox,
  ComboboxContent,
  ComboboxEmpty,
  ComboboxInput,
  ComboboxItem,
  ComboboxList,
  ComboboxTrigger,
} from "@/components/ui/combobox";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
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

export function FilterBar<TData>({ fields, rows, value, onChange }: FilterBarProps<TData>) {
  return (
    <>
      <AddFilterMenu fields={fields} rows={rows} value={value} onChange={onChange} />
      {value.map((rule) => {
        const field = fields.find((candidate) => candidate.id === rule.id);
        return field ? (
          <FilterChip
            key={rule.id}
            field={field}
            rule={rule}
            fields={fields}
            rows={rows}
            value={value}
            onChange={onChange}
          />
        ) : null;
      })}
      {value.length > 0 ? (
        <Button variant="ghost" className="text-muted-foreground" onClick={() => onChange([])}>
          Clear
        </Button>
      ) : null}
    </>
  );
}

/** Picks a field, then toggles its values, inside one combobox popup. */
function AddFilterMenu<TData>({ fields, rows, value, onChange }: FilterBarProps<TData>) {
  const [open, setOpen] = useState(false);
  const [fieldId, setFieldId] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const field = fields.find((candidate) => candidate.id === fieldId);
  const selected = field ? (value.find((rule) => rule.id === field.id)?.values ?? []) : [];
  const counts = field ? facetCounts(rows, fields, value, field) : null;

  function handleOpenChange(next: boolean) {
    setOpen(next);
    if (!next) {
      setFieldId(null);
      setQuery("");
    }
  }

  function handleValueChange(next: string[]) {
    if (field) {
      onChange(setFilterValues(value, field.id, next));
      return;
    }
    setFieldId(next.at(-1) ?? null);
    setQuery("");
  }

  function handleKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (field && event.key === "Backspace" && event.currentTarget.value === "") {
      event.preventDefault();
      setFieldId(null);
    }
  }

  function label(item: string) {
    if (field) return field.options.find((option) => option.value === item)?.label ?? item;
    return fields.find((candidate) => candidate.id === item)?.label ?? item;
  }

  return (
    <Combobox
      multiple
      autoHighlight
      items={field ? field.options.map((option) => option.value) : fields.map(({ id }) => id)}
      value={selected}
      onValueChange={handleValueChange}
      itemToStringLabel={label}
      open={open}
      onOpenChange={handleOpenChange}
      inputValue={query}
      onInputValueChange={setQuery}
    >
      <ComboboxPrimitive.Trigger
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
      </ComboboxPrimitive.Trigger>
      <ComboboxContent className="min-w-56">
        <ComboboxInput
          showTrigger={false}
          placeholder={field ? `${field.label}…` : "Filter…"}
          onKeyDown={handleKeyDown}
        />
        <ComboboxEmpty>No matches.</ComboboxEmpty>
        <ComboboxList>
          {(item: string) => (
            <ComboboxItem key={item} value={item}>
              <span className="truncate">{label(item)}</span>
              {counts ? <OptionCount count={counts.get(item) ?? 0} /> : null}
            </ComboboxItem>
          )}
        </ComboboxList>
      </ComboboxContent>
    </Combobox>
  );
}

function OptionCount({ count }: { count: number }) {
  return <span className="ml-auto text-xs text-muted-foreground tabular-nums">{count}</span>;
}

const SEGMENT =
  "flex h-full items-center gap-1.5 px-2 outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:bg-muted focus-visible:text-foreground aria-expanded:bg-muted aria-expanded:text-foreground";

function FilterChip<TData>({
  field,
  rule,
  fields,
  rows,
  value,
  onChange,
}: FilterBarProps<TData> & { field: FilterField<TData>; rule: FilterRule }) {
  const selected = field.options.filter((option) => rule.values.includes(option.value));
  const counts = facetCounts(rows, fields, value, field);

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

      <Combobox
        multiple
        autoHighlight
        items={field.options.map((option) => option.value)}
        value={rule.values}
        onValueChange={(next) => onChange(setFilterValues(value, field.id, next))}
        itemToStringLabel={(item) =>
          field.options.find((option) => option.value === item)?.label ?? item
        }
      >
        <ComboboxTrigger
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
        </ComboboxTrigger>
        <ComboboxContent className="min-w-56">
          <ComboboxInput showTrigger={false} placeholder={`${field.label}…`} />
          <ComboboxEmpty>No matches.</ComboboxEmpty>
          <ComboboxList>
            {(item: string) => (
              <ComboboxItem key={item} value={item}>
                <span className="truncate">
                  {field.options.find((option) => option.value === item)?.label}
                </span>
                <OptionCount count={counts.get(item) ?? 0} />
              </ComboboxItem>
            )}
          </ComboboxList>
        </ComboboxContent>
      </Combobox>

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
