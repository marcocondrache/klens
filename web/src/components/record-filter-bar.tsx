import { useMemo, useState } from "react";
import { format } from "date-fns";
import {
  ChevronDownIcon,
  ChevronRightIcon,
  ListFilterIcon,
  XIcon,
} from "lucide-react";
import type { DateRange } from "react-day-picker";

import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import type { SchemaSubject } from "@/lib/api/types";
import {
  CREATED_PRESETS,
  createdLabel,
  defaultCustomRange,
  type CreatedPresetId,
  type RecordFilterKey,
  type RecordFilterState,
} from "@/lib/record-filters";
import { cn } from "@/lib/utils";

const DECODABLE = new Set(["AVRO", "JSON", "PROTOBUF"]);
const ORDER_OPTIONS = [
  { value: "NEWEST" as const, label: "Newest first" },
  { value: "OLDEST" as const, label: "Oldest first" },
];
const LIMIT_OPTIONS = ["25", "50", "100"] as const;

type Panel =
  | { view: "fields" }
  | { view: "value"; key: RecordFilterKey }
  | { view: "custom-created" };

type Option = {
  value: string;
  label: string;
  checked?: boolean;
  trailing?: "chevron";
  onSelect: () => void;
};

function FilterChip({
  label,
  value,
  active,
  onClick,
  onClear,
}: {
  label: string;
  value: string;
  active?: boolean;
  onClick: () => void;
  onClear: () => void;
}) {
  return (
    <div
      className={cn(
        "inline-flex h-8 max-w-64 items-center overflow-hidden rounded-[min(var(--radius-md),12px)] border border-dashed border-muted-foreground/45",
        active && "border-solid border-border bg-muted/40",
      )}
    >
      <Button
        type="button"
        variant="ghost"
        size="sm"
        className="h-8 max-w-full min-w-0 gap-1.5 rounded-none px-2.5 font-normal"
        onClick={onClick}
      >
        <span className="shrink-0 text-muted-foreground">{label}</span>
        <span className="min-w-0 truncate font-medium">{value}</span>
      </Button>
      <Button
        type="button"
        variant="ghost"
        size="icon-sm"
        className="size-8 shrink-0 rounded-none"
        aria-label={`Clear ${label} filter`}
        onClick={onClear}
      >
        <XIcon />
      </Button>
    </div>
  );
}

function FilterHeader({
  label,
  onBack,
  children,
}: {
  label: string;
  onBack: () => void;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-center border-b">
      <Button
        type="button"
        variant="ghost"
        size="sm"
        className="h-9 shrink-0 rounded-none border-r px-2.5 font-medium"
        onClick={onBack}
      >
        {label}
        <ChevronDownIcon data-icon="inline-end" />
      </Button>
      <div className="flex min-w-0 flex-1 items-center px-2 text-sm">{children}</div>
    </div>
  );
}

function OptionItems({ options }: { options: Option[] }) {
  return (
    <CommandGroup>
      {options.map((option) => (
        <CommandItem
          key={option.value}
          value={option.value}
          data-checked={option.checked || undefined}
          onSelect={option.onSelect}
        >
          {option.label}
          {option.trailing === "chevron" ? (
            <ChevronRightIcon className="ml-auto size-4 text-muted-foreground opacity-100!" />
          ) : null}
        </CommandItem>
      ))}
    </CommandGroup>
  );
}

export function RecordFilterBar({
  value,
  onChange,
  partitions,
  subjects,
  topic,
  showSchema,
  className,
}: {
  value: RecordFilterState;
  onChange: (next: RecordFilterState) => void;
  partitions: Array<{ id: number }>;
  subjects: SchemaSubject[];
  topic: string;
  showSchema: boolean;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const [panel, setPanel] = useState<Panel>({ view: "fields" });
  const [searchDraft, setSearchDraft] = useState(value.search);
  const [customRange, setCustomRange] = useState<DateRange | undefined>(() =>
    value.created?.kind === "custom"
      ? { from: value.created.from, to: value.created.to }
      : defaultCustomRange(),
  );

  const schemas = useMemo(() => {
    const preferred = `${topic}-value`;
    const decodable = subjects.filter((subject) => DECODABLE.has(subject.type));
    return {
      pinned: decodable.filter((subject) => subject.subject === preferred),
      rest: decodable.filter((subject) => subject.subject !== preferred),
      all: decodable,
    };
  }, [subjects, topic]);

  const fields = useMemo(() => {
    const items: Array<{ key: RecordFilterKey; label: string }> = [
      { key: "created", label: "Created" },
      { key: "search", label: "Search" },
      { key: "partition", label: "Partition" },
      { key: "order", label: "Order" },
      { key: "limit", label: "Limit" },
    ];
    if (showSchema && schemas.all.length > 0) items.push({ key: "schemaId", label: "Schema" });
    return items;
  }, [schemas.all.length, showSchema]);

  const activeKey: RecordFilterKey | null =
    panel.view === "fields" ? null : panel.view === "custom-created" ? "created" : panel.key;
  const activeLabel = fields.find((field) => field.key === activeKey)?.label ?? "Filter";

  function patch(partial: Partial<RecordFilterState>) {
    onChange({ ...value, ...partial });
  }

  function clear(key: RecordFilterKey) {
    const empty: RecordFilterState = {
      created: null,
      search: "",
      partition: null,
      order: null,
      limit: null,
      schemaId: null,
    };
    if (key === "search") setSearchDraft("");
    patch({ [key]: empty[key] });
  }

  function close() {
    setOpen(false);
    setPanel({ view: "fields" });
  }

  function openField(key: RecordFilterKey) {
    setSearchDraft(value.search);
    if (key === "created" && value.created?.kind === "custom") {
      setCustomRange({ from: value.created.from, to: value.created.to });
      setPanel({ view: "custom-created" });
    } else {
      setPanel({ view: "value", key });
    }
    setOpen(true);
  }

  function chooseCreated(id: CreatedPresetId | null) {
    if (id == null) clear("created");
    else patch({ created: { kind: "preset", id } });
    close();
  }

  const chips = useMemo(() => {
    const next: Array<{ key: RecordFilterKey; label: string; display: string }> = [];
    if (value.created) next.push({ key: "created", label: "Created", display: createdLabel(value.created) });
    if (value.search.trim()) next.push({ key: "search", label: "Search", display: value.search.trim() });
    if (value.partition != null) {
      next.push({ key: "partition", label: "Partition", display: value.partition });
    }
    if (value.order) {
      next.push({
        key: "order",
        label: "Order",
        display: ORDER_OPTIONS.find((item) => item.value === value.order)?.label ?? value.order,
      });
    }
    if (value.limit) next.push({ key: "limit", label: "Limit", display: `${value.limit} rows` });
    if (value.schemaId != null) {
      next.push({
        key: "schemaId",
        label: "Schema",
        display: schemas.all.find((item) => item.id === value.schemaId)?.subject ?? String(value.schemaId),
      });
    }
    return next;
  }, [schemas.all, value]);

  const valueOptions: Option[] = (() => {
    switch (activeKey) {
      case "created":
        return [
          {
            value: "any date",
            label: "Any Date",
            checked: value.created == null,
            onSelect: () => chooseCreated(null),
          },
          ...CREATED_PRESETS.map((preset) => ({
            value: preset.label,
            label: preset.label,
            checked: value.created?.kind === "preset" && value.created.id === preset.id,
            onSelect: () => chooseCreated(preset.id),
          })),
        ];
      case "search":
        return [
          {
            value: "apply search",
            label: "Apply search",
            onSelect: () => {
              const next = searchDraft.trim();
              patch({ search: next });
              if (!next) clear("search");
              close();
            },
          },
        ];
      case "partition":
        return [
          {
            value: "all partitions",
            label: "All partitions",
            checked: value.partition == null,
            onSelect: () => {
              clear("partition");
              close();
            },
          },
          ...partitions.map((part) => ({
            value: `partition ${part.id}`,
            label: `Partition ${part.id}`,
            checked: value.partition === String(part.id),
            onSelect: () => {
              patch({ partition: String(part.id) });
              close();
            },
          })),
        ];
      case "order":
        return ORDER_OPTIONS.map((option) => ({
          value: option.label,
          label: option.label,
          checked: (value.order ?? "NEWEST") === option.value,
          onSelect: () => {
            patch({ order: option.value === "NEWEST" ? null : option.value });
            close();
          },
        }));
      case "limit":
        return LIMIT_OPTIONS.map((limit) => ({
          value: `${limit} rows`,
          label: `${limit} rows`,
          checked: (value.limit ?? "50") === limit,
          onSelect: () => {
            patch({ limit: limit === "50" ? null : limit });
            close();
          },
        }));
      default:
        return [];
    }
  })();

  return (
    <div className={cn("flex min-w-0 flex-wrap items-center gap-2", className)}>
      <Popover
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) setPanel({ view: "fields" });
          else setSearchDraft(value.search);
        }}
      >
        <PopoverTrigger render={<Button variant="outline" size="sm" aria-label="Add filter" />}>
          <ListFilterIcon data-icon="inline-start" />
          Add Filter
        </PopoverTrigger>
        <PopoverContent
          align="start"
          className={cn("gap-0 overflow-hidden p-0", panel.view === "custom-created" && "w-auto")}
        >
          {panel.view === "fields" ? (
            <Command>
              <CommandInput placeholder="Filter by…" />
              <CommandList>
                <CommandEmpty>No matching filters.</CommandEmpty>
                <OptionItems
                  options={fields.map((field) => ({
                    value: field.label,
                    label: field.label,
                    trailing: "chevron",
                    onSelect: () => openField(field.key),
                  }))}
                />
              </CommandList>
            </Command>
          ) : panel.view === "custom-created" ? (
            <div className="flex flex-col">
              <FilterHeader label={activeLabel} onBack={() => setPanel({ view: "fields" })}>
                {customRange?.from
                  ? customRange.to
                    ? `${format(customRange.from, "LLL d")} – ${format(customRange.to, "LLL d")}`
                    : format(customRange.from, "LLL d")
                  : "Pick a range"}
              </FilterHeader>
              <Calendar
                mode="range"
                selected={customRange}
                onSelect={setCustomRange}
                defaultMonth={customRange?.from}
                className="bg-transparent"
              />
              <div className="border-t p-2">
                <Button
                  size="sm"
                  className="w-full"
                  disabled={!customRange?.from || !customRange.to}
                  onClick={() => {
                    if (!customRange?.from || !customRange.to) return;
                    patch({
                      created: { kind: "custom", from: customRange.from, to: customRange.to },
                    });
                    close();
                  }}
                >
                  Apply
                </Button>
              </div>
            </div>
          ) : (
            <Command>
              <FilterHeader label={activeLabel} onBack={() => setPanel({ view: "fields" })}>
                {activeKey === "search" ? (
                  <Input
                    data-search-hotkey
                    value={searchDraft}
                    onChange={(event) => setSearchDraft(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key !== "Enter") return;
                      event.preventDefault();
                      const next = searchDraft.trim();
                      patch({ search: next });
                      if (!next) clear("search");
                      close();
                    }}
                    placeholder="Filter to…"
                    className="h-8 border-0 bg-transparent shadow-none focus-visible:ring-0 dark:bg-transparent"
                    autoFocus
                  />
                ) : (
                  <span className="text-muted-foreground">Filter to…</span>
                )}
              </FilterHeader>
              <CommandList>
                <CommandEmpty>No results.</CommandEmpty>
                {activeKey === "schemaId" ? (
                  <>
                    <OptionItems
                      options={[
                        {
                          value: "raw no schema",
                          label: "Raw",
                          checked: value.schemaId == null,
                          onSelect: () => {
                            clear("schemaId");
                            close();
                          },
                        },
                      ]}
                    />
                    {schemas.pinned.length > 0 ? (
                      <CommandGroup heading="Suggested">
                        {schemas.pinned.map((subject) => (
                          <CommandItem
                            key={subject.subject}
                            value={`${subject.subject} ${subject.type}`}
                            data-checked={subject.id === value.schemaId || undefined}
                            onSelect={() => {
                              patch({ schemaId: subject.id });
                              close();
                            }}
                          >
                            <span className="truncate font-mono text-sm">{subject.subject}</span>
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    ) : null}
                    {schemas.rest.length > 0 ? (
                      <CommandGroup heading={schemas.pinned.length > 0 ? "All" : undefined}>
                        {schemas.rest.map((subject) => (
                          <CommandItem
                            key={subject.subject}
                            value={`${subject.subject} ${subject.type}`}
                            data-checked={subject.id === value.schemaId || undefined}
                            onSelect={() => {
                              patch({ schemaId: subject.id });
                              close();
                            }}
                          >
                            <span className="truncate font-mono text-sm">{subject.subject}</span>
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    ) : null}
                  </>
                ) : (
                  <>
                    <OptionItems options={valueOptions} />
                    {activeKey === "created" ? (
                      <>
                        <CommandSeparator />
                        <OptionItems
                          options={[
                            {
                              value: "custom date range",
                              label: "Custom Date Range",
                              trailing: "chevron",
                              onSelect: () => {
                                setCustomRange(
                                  value.created?.kind === "custom"
                                    ? { from: value.created.from, to: value.created.to }
                                    : defaultCustomRange(),
                                );
                                setPanel({ view: "custom-created" });
                              },
                            },
                          ]}
                        />
                      </>
                    ) : null}
                  </>
                )}
              </CommandList>
            </Command>
          )}
        </PopoverContent>
      </Popover>

      {chips.map((chip) => (
        <FilterChip
          key={chip.key}
          label={chip.label}
          value={chip.display}
          active={open && activeKey === chip.key}
          onClick={() => openField(chip.key)}
          onClear={() => clear(chip.key)}
        />
      ))}

      {!open || activeKey !== "search" ? (
        <input
          data-search-hotkey
          className="sr-only"
          tabIndex={-1}
          aria-hidden
          onFocus={() => openField("search")}
        />
      ) : null}
    </div>
  );
}
