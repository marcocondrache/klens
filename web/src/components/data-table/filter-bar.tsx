import { useState, type KeyboardEvent } from "react";
import { CheckIcon, ChevronDownIcon, ChevronRightIcon, ListFilterIcon, XIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "@/components/ui/command";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";

import {
  facetCounts,
  operatorLabel,
  toggleFilterValue,
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

function AddFilterMenu<TData>({ fields, rows, value, onChange }: FilterBarProps<TData>) {
  const [open, setOpen] = useState(false);
  const [fieldId, setFieldId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const field = fields.find((candidate) => candidate.id === fieldId);

  function handleOpenChange(next: boolean) {
    setOpen(next);
    if (!next) {
      setFieldId(null);
      setSearch("");
    }
  }

  return (
    <Popover open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger
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
      </PopoverTrigger>
      <PopoverContent align="start" className="w-60 gap-0 p-0">
        {field ? (
          <OptionList
            field={field}
            fields={fields}
            rows={rows}
            value={value}
            onChange={onChange}
            onBack={() => setFieldId(null)}
          />
        ) : (
          <Command>
            <CommandInput
              placeholder="Filter…"
              value={search}
              onValueChange={setSearch}
              autoFocus
            />
            <CommandList>
              <CommandEmpty>No matching filters.</CommandEmpty>
              <CommandGroup>
                {fields.map((candidate) => (
                  <CommandItem
                    key={candidate.id}
                    value={candidate.label}
                    onSelect={() => {
                      setFieldId(candidate.id);
                      setSearch("");
                    }}
                  >
                    <candidate.icon className="text-muted-foreground" />
                    {candidate.label}
                    <CommandShortcut>
                      <ChevronRightIcon className="size-3.5" />
                    </CommandShortcut>
                  </CommandItem>
                ))}
              </CommandGroup>
              {search.trim()
                ? fields.map((candidate) => {
                    const rule = value.find((existing) => existing.id === candidate.id);
                    return (
                      <CommandGroup key={candidate.id} heading={candidate.label}>
                        {candidate.options.map((option) => (
                          <CommandItem
                            key={option.value}
                            value={`${candidate.label} ${option.label}`}
                            data-checked={rule?.values.includes(option.value) || undefined}
                            onSelect={() => {
                              onChange(toggleFilterValue(value, candidate.id, option.value));
                              handleOpenChange(false);
                            }}
                          >
                            {option.icon}
                            <span className="truncate">{option.label}</span>
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    );
                  })
                : null}
            </CommandList>
          </Command>
        )}
      </PopoverContent>
    </Popover>
  );
}

function OptionList<TData>({
  field,
  onBack,
  ...props
}: FilterBarProps<TData> & { field: FilterField<TData>; onBack?: () => void }) {
  const { fields, rows, value, onChange } = props;
  const rule = value.find((existing) => existing.id === field.id);
  const counts = facetCounts(rows, fields, value, field);

  function handleKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (onBack && event.key === "Backspace" && event.currentTarget.value === "") {
      event.preventDefault();
      onBack();
    }
  }

  return (
    <Command>
      <CommandInput placeholder={`${field.label}…`} autoFocus onKeyDown={handleKeyDown} />
      <CommandList>
        <CommandEmpty>No matching {field.plural}.</CommandEmpty>
        <CommandGroup>
          {field.options.map((option) => {
            const checked = rule?.values.includes(option.value) ?? false;
            return (
              <CommandItem
                key={option.value}
                value={option.label}
                aria-checked={checked}
                onSelect={() => onChange(toggleFilterValue(value, field.id, option.value))}
              >
                <span
                  className={cn(
                    "flex size-4 shrink-0 items-center justify-center rounded-[4px] border border-input transition-colors",
                    checked && "border-primary bg-primary text-primary-foreground",
                  )}
                >
                  {checked ? <CheckIcon className="size-3" /> : null}
                </span>
                {option.icon}
                <span className="truncate">{option.label}</span>
                <CommandShortcut className="tracking-normal tabular-nums">
                  {counts.get(option.value) ?? 0}
                </CommandShortcut>
              </CommandItem>
            );
          })}
        </CommandGroup>
      </CommandList>
    </Command>
  );
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

      <Popover>
        <PopoverTrigger className={cn(SEGMENT, "font-medium")}>
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
        </PopoverTrigger>
        <PopoverContent align="start" className="w-60 gap-0 p-0">
          <OptionList field={field} {...props} />
        </PopoverContent>
      </Popover>

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
