import { useMemo, useState } from "react";
import {
  CheckIcon,
  ChevronDownIcon,
  ChevronRightIcon,
  ListFilterIcon,
  XIcon,
} from "lucide-react";

import { DateRangeFilter } from "@/components/date-range-filter";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Separator } from "@/components/ui/separator";
import type { SchemaSubject, RecordOrder } from "@/lib/api/types";
import {
  endOfLocalDay,
  formatDateRangeLabel,
  startOfLocalDay,
} from "@/lib/format";
import {
  CREATED_PRESETS,
  createdLabel,
  type CreatedPresetId,
  type DateRangeValue,
  type RecordFilterKey,
  type RecordFilterState,
} from "@/lib/record-filters";
import { cn } from "@/lib/utils";

const DECODABLE = new Set(["AVRO", "JSON", "PROTOBUF"]);

const ORDER_OPTIONS: Array<{ value: RecordOrder; label: string }> = [
  { value: "NEWEST", label: "Newest first" },
  { value: "OLDEST", label: "Oldest first" },
];

const LIMIT_OPTIONS = ["25", "50", "100"] as const;

type Panel =
  | { view: "fields" }
  | { view: "value"; key: RecordFilterKey }
  | { view: "custom-created" };

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
        "inline-flex h-8 max-w-64 items-center gap-1 rounded-[min(var(--radius-md),12px)] border border-dashed border-muted-foreground/45 bg-transparent px-1",
        active && "border-solid border-border bg-muted/40",
      )}
    >
      <button
        type="button"
        onClick={onClick}
        className="inline-flex min-w-0 items-center gap-1.5 rounded-[min(var(--radius-md),12px)] px-2 py-1 text-sm"
      >
        <span className="shrink-0 text-muted-foreground">{label}</span>
        <span className="min-w-0 truncate font-medium">{value}</span>
      </button>
      <button
        type="button"
        onClick={onClear}
        className="inline-flex size-6 shrink-0 items-center justify-center rounded-[min(var(--radius-md),10px)] text-muted-foreground hover:bg-muted hover:text-foreground"
        aria-label={`Clear ${label} filter`}
      >
        <XIcon className="size-3.5" />
      </button>
    </div>
  );
}

function OptionRow({
  label,
  selected,
  onSelect,
  trailing,
}: {
  label: string;
  selected?: boolean;
  onSelect: () => void;
  trailing?: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        "flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm outline-none hover:bg-muted",
        selected && "bg-muted/60",
      )}
    >
      <span className="min-w-0 flex-1 truncate">{label}</span>
      {trailing ?? (selected ? <CheckIcon className="size-4 shrink-0" /> : null)}
    </button>
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
  const [fieldQuery, setFieldQuery] = useState("");
  const [searchDraft, setSearchDraft] = useState(value.search);

  const schemaOptions = useMemo(() => {
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
    if (showSchema && schemaOptions.all.length > 0) {
      items.push({ key: "schemaId", label: "Schema" });
    }
    return items;
  }, [schemaOptions.all.length, showSchema]);

  const activeKey: RecordFilterKey | null =
    panel.view === "fields" ? null : panel.view === "custom-created" ? "created" : panel.key;
  const activeField = fields.find((field) => field.key === activeKey) ?? null;

  function patch(partial: Partial<RecordFilterState>) {
    onChange({ ...value, ...partial });
  }

  function clear(key: RecordFilterKey) {
    switch (key) {
      case "created":
        patch({ created: null });
        break;
      case "search":
        patch({ search: "" });
        setSearchDraft("");
        break;
      case "partition":
        patch({ partition: null });
        break;
      case "order":
        patch({ order: null });
        break;
      case "limit":
        patch({ limit: null });
        break;
      case "schemaId":
        patch({ schemaId: null });
        break;
    }
  }

  function openField(key: RecordFilterKey) {
    setSearchDraft(value.search);
    setFieldQuery("");
    if (key === "created" && value.created?.kind === "custom") {
      setPanel({ view: "custom-created" });
    } else {
      setPanel({ view: "value", key });
    }
    setOpen(true);
  }

  function closePopover() {
    setOpen(false);
    setPanel({ view: "fields" });
    setFieldQuery("");
  }

  const visibleFields = fields.filter((field) =>
    field.label.toLowerCase().includes(fieldQuery.trim().toLowerCase()),
  );

  function selectCreatedPreset(id: CreatedPresetId | "any") {
    if (id === "any") {
      clear("created");
      closePopover();
      return;
    }
    patch({ created: { kind: "preset", id } });
    closePopover();
  }

  function applyCustomCreated(range: DateRangeValue) {
    patch({
      created: {
        kind: "custom",
        from: range.from,
        to: range.to,
      },
    });
    closePopover();
  }

  function applySearch() {
    const next = searchDraft.trim();
    patch({ search: next });
    if (!next) clear("search");
    closePopover();
  }

  const chips: Array<{ key: RecordFilterKey; label: string; display: string }> = [];
  if (value.created) {
    chips.push({ key: "created", label: "Created", display: createdLabel(value.created) });
  }
  if (value.search.trim()) {
    chips.push({ key: "search", label: "Search", display: value.search.trim() });
  }
  if (value.partition != null) {
    chips.push({ key: "partition", label: "Partition", display: value.partition });
  }
  if (value.order) {
    chips.push({
      key: "order",
      label: "Order",
      display: ORDER_OPTIONS.find((item) => item.value === value.order)?.label ?? value.order,
    });
  }
  if (value.limit) {
    chips.push({ key: "limit", label: "Limit", display: `${value.limit} rows` });
  }
  if (value.schemaId != null) {
    const subject = schemaOptions.all.find((item) => item.id === value.schemaId);
    chips.push({
      key: "schemaId",
      label: "Schema",
      display: subject?.subject ?? String(value.schemaId),
    });
  }

  const headerPreview = (() => {
    if (activeKey === "created") {
      if (panel.view === "custom-created") {
        const custom = value.created?.kind === "custom" ? value.created : null;
        if (custom) return formatDateRangeLabel(custom.from, custom.to);
        return "Custom range";
      }
      return "Filter to…";
    }
    if (activeKey === "search") return undefined;
    return "Filter to…";
  })();

  return (
    <div className={cn("flex min-w-0 flex-wrap items-center gap-2", className)}>
      <Popover
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) {
            setPanel({ view: "fields" });
            setFieldQuery("");
          }
          if (next) setSearchDraft(value.search);
        }}
      >
        <PopoverTrigger
          render={
            <Button variant="outline" size="sm" aria-label="Add filter" />
          }
        >
          <ListFilterIcon data-icon="inline-start" />
          Add Filter
        </PopoverTrigger>
        <PopoverContent
          align="start"
          side="bottom"
          className={cn(
            "gap-0 overflow-hidden p-0",
            panel.view === "custom-created" ? "w-[22rem]" : "w-72",
          )}
        >
          {panel.view === "fields" ? (
            <div className="flex flex-col">
              <div className="border-b p-1">
                <Input
                  value={fieldQuery}
                  onChange={(event) => setFieldQuery(event.target.value)}
                  placeholder="Filter by…"
                  className="h-8 border-0 bg-transparent shadow-none focus-visible:ring-0 dark:bg-transparent"
                  autoFocus
                />
              </div>
              <div className="flex flex-col gap-0.5 p-1">
                {visibleFields.length === 0 ? (
                  <p className="px-2 py-6 text-center text-sm text-muted-foreground">
                    No matching filters.
                  </p>
                ) : (
                  visibleFields.map((field) => (
                    <OptionRow
                      key={field.key}
                      label={field.label}
                      onSelect={() => openField(field.key)}
                      trailing={<ChevronRightIcon className="size-4 text-muted-foreground" />}
                    />
                  ))
                )}
              </div>
            </div>
          ) : (
            <div className="flex flex-col">
              <div className="flex items-center gap-0 border-b">
                <DropdownMenu>
                  <DropdownMenuTrigger
                    render={
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-9 shrink-0 rounded-none border-r px-2.5 font-medium"
                      />
                    }
                  >
                    {activeField?.label ?? "Filter"}
                    <ChevronDownIcon data-icon="inline-end" />
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start" className="min-w-40">
                    <DropdownMenuGroup>
                      {fields.map((field) => (
                        <DropdownMenuItem key={field.key} onClick={() => openField(field.key)}>
                          {field.label}
                        </DropdownMenuItem>
                      ))}
                    </DropdownMenuGroup>
                  </DropdownMenuContent>
                </DropdownMenu>

                {activeKey === "search" ? (
                  <Input
                    data-search-hotkey
                    value={searchDraft}
                    onChange={(event) => setSearchDraft(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        event.preventDefault();
                        applySearch();
                      }
                    }}
                    placeholder="Filter to…"
                    className="h-9 rounded-none border-0 bg-transparent shadow-none focus-visible:ring-0 dark:bg-transparent"
                    autoFocus
                  />
                ) : (
                  <div className="flex h-9 min-w-0 flex-1 items-center px-2.5 text-sm text-muted-foreground">
                    <span className="truncate">{headerPreview}</span>
                  </div>
                )}
              </div>

              {panel.view === "custom-created" ? (
                <DateRangeFilter
                  value={
                    value.created?.kind === "custom"
                      ? { from: value.created.from, to: value.created.to }
                      : {
                          from: startOfLocalDay(new Date()),
                          to: endOfLocalDay(new Date()),
                        }
                  }
                  onApply={applyCustomCreated}
                />
              ) : activeKey === "created" ? (
                <div className="flex flex-col gap-0.5 p-1">
                  <OptionRow
                    label="Any Date"
                    selected={value.created == null}
                    onSelect={() => selectCreatedPreset("any")}
                  />
                  {CREATED_PRESETS.map((preset) => (
                    <OptionRow
                      key={preset.id}
                      label={preset.label}
                      selected={
                        value.created?.kind === "preset" && value.created.id === preset.id
                      }
                      onSelect={() => selectCreatedPreset(preset.id)}
                    />
                  ))}
                  <Separator className="my-1" />
                  <OptionRow
                    label="Custom Date Range"
                    selected={value.created?.kind === "custom"}
                    onSelect={() => setPanel({ view: "custom-created" })}
                    trailing={<ChevronRightIcon className="size-4 text-muted-foreground" />}
                  />
                </div>
              ) : activeKey === "search" ? (
                <div className="flex flex-col gap-2 p-2">
                  <p className="px-1 text-xs text-muted-foreground">
                    Match text in record keys or values.
                  </p>
                  <Button size="sm" onClick={applySearch}>
                    Apply search
                  </Button>
                </div>
              ) : activeKey === "partition" ? (
                <div className="flex max-h-72 flex-col gap-0.5 overflow-y-auto p-1">
                  <OptionRow
                    label="All partitions"
                    selected={value.partition == null}
                    onSelect={() => {
                      clear("partition");
                      closePopover();
                    }}
                  />
                  {partitions.map((part) => (
                    <OptionRow
                      key={part.id}
                      label={`Partition ${part.id}`}
                      selected={value.partition === String(part.id)}
                      onSelect={() => {
                        patch({ partition: String(part.id) });
                        closePopover();
                      }}
                    />
                  ))}
                </div>
              ) : activeKey === "order" ? (
                <div className="flex flex-col gap-0.5 p-1">
                  {ORDER_OPTIONS.map((option) => (
                    <OptionRow
                      key={option.value}
                      label={option.label}
                      selected={(value.order ?? "NEWEST") === option.value}
                      onSelect={() => {
                        patch({
                          order: option.value === "NEWEST" ? null : option.value,
                        });
                        closePopover();
                      }}
                    />
                  ))}
                </div>
              ) : activeKey === "limit" ? (
                <div className="flex flex-col gap-0.5 p-1">
                  {LIMIT_OPTIONS.map((limit) => (
                    <OptionRow
                      key={limit}
                      label={`${limit} rows`}
                      selected={(value.limit ?? "50") === limit}
                      onSelect={() => {
                        patch({ limit: limit === "50" ? null : limit });
                        closePopover();
                      }}
                    />
                  ))}
                </div>
              ) : activeKey === "schemaId" ? (
                <Command>
                  <CommandInput placeholder="Search subjects…" />
                  <CommandList>
                    <CommandEmpty>No matching subjects.</CommandEmpty>
                    <CommandGroup>
                      <CommandItem
                        value="raw no schema"
                        data-checked={value.schemaId == null || undefined}
                        onSelect={() => {
                          clear("schemaId");
                          closePopover();
                        }}
                      >
                        Raw
                      </CommandItem>
                    </CommandGroup>
                    {schemaOptions.pinned.length > 0 ? (
                      <CommandGroup heading="Suggested">
                        {schemaOptions.pinned.map((subject) => (
                          <CommandItem
                            key={subject.subject}
                            value={`${subject.subject} ${subject.type} ${subject.id}`}
                            data-checked={subject.id === value.schemaId || undefined}
                            onSelect={() => {
                              patch({ schemaId: subject.id });
                              closePopover();
                            }}
                          >
                            <span className="min-w-0 flex-1 truncate font-mono text-sm">
                              {subject.subject}
                            </span>
                            <span className="shrink-0 text-xs text-muted-foreground">
                              {subject.type} · v{subject.latestVersion}
                            </span>
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    ) : null}
                    {schemaOptions.rest.length > 0 ? (
                      <CommandGroup heading={schemaOptions.pinned.length > 0 ? "All" : undefined}>
                        {schemaOptions.rest.map((subject) => (
                          <CommandItem
                            key={subject.subject}
                            value={`${subject.subject} ${subject.type} ${subject.id}`}
                            data-checked={subject.id === value.schemaId || undefined}
                            onSelect={() => {
                              patch({ schemaId: subject.id });
                              closePopover();
                            }}
                          >
                            <span className="min-w-0 flex-1 truncate font-mono text-sm">
                              {subject.subject}
                            </span>
                            <span className="shrink-0 text-xs text-muted-foreground">
                              {subject.type} · v{subject.latestVersion}
                            </span>
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    ) : null}
                  </CommandList>
                </Command>
              ) : null}
            </div>
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

      {/* Keep a hidden hotkey target when search isn't open */}
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
