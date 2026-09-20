import { useState } from "react";
import { ChevronsUpDownIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { useSubjectRows } from "@/lib/api/catalog";
import type { SubjectRow } from "@/lib/api/types";

const DECODABLE = new Set(["AVRO", "JSON", "PROTOBUF"]);

const TRIGGER_BUTTON = {
  variant: "outline" as const,
  className: "max-w-64 min-w-0 gap-1 font-normal",
  "aria-label": "Decode value with schema",
};

export type FramingRecord = {
  value: string | null;
  schemaId: number | null;
};

type SchemaPickerIntent = { kind: "dormant" } | { kind: "active"; binding: number | null };

type SchemaChoice = {
  id: number;
  subject: string;
  type: string;
  latestVersion: number;
};

type SchemaMenu = {
  pinned: SchemaChoice[];
  rest: SchemaChoice[];
};

type SchemaPickerLoad =
  | { kind: "pending" }
  | { kind: "empty" }
  | { kind: "ready"; menu: SchemaMenu };

type SchemaTrigger =
  | { kind: "prompt" }
  | { kind: "choice"; choice: SchemaChoice }
  | { kind: "unresolved"; id: number };

type SchemaPickerView =
  | { kind: "hidden" }
  | { kind: "pending"; binding: number | null }
  | { kind: "ready"; trigger: SchemaTrigger; menu: SchemaMenu };

function pickerIntent(binding: number | null, page: readonly FramingRecord[]): SchemaPickerIntent {
  if (binding != null) return { kind: "active", binding };
  if (page.some((row) => row.value != null && row.schemaId == null)) {
    return { kind: "active", binding: null };
  }
  return { kind: "dormant" };
}

function projectMenu(rows: readonly SubjectRow[], topic: string): SchemaMenu | null {
  const preferred = `${topic}-value`;
  const pinned: SchemaChoice[] = [];
  const rest: SchemaChoice[] = [];
  for (const row of rows) {
    if (!DECODABLE.has(row.type)) continue;
    const choice: SchemaChoice = {
      id: row.id,
      subject: row.subject,
      type: row.type,
      latestVersion: row.latestVersion,
    };
    if (row.subject === preferred) pinned.push(choice);
    else rest.push(choice);
  }
  if (pinned.length === 0 && rest.length === 0) return null;
  return { pinned, rest };
}

function triggerFor(binding: number | null, menu: SchemaMenu): SchemaTrigger {
  if (binding == null) return { kind: "prompt" };
  const choice =
    menu.pinned.find((item) => item.id === binding) ??
    menu.rest.find((item) => item.id === binding);
  return choice ? { kind: "choice", choice } : { kind: "unresolved", id: binding };
}

function schemaPickerView(intent: SchemaPickerIntent, load: SchemaPickerLoad): SchemaPickerView {
  if (intent.kind === "dormant") return { kind: "hidden" };
  switch (load.kind) {
    case "pending":
      return { kind: "pending", binding: intent.binding };
    case "empty":
      if (intent.binding == null) return { kind: "hidden" };
      return {
        kind: "ready",
        trigger: { kind: "unresolved", id: intent.binding },
        menu: { pinned: [], rest: [] },
      };
    case "ready":
      return {
        kind: "ready",
        trigger: triggerFor(intent.binding, load.menu),
        menu: load.menu,
      };
  }
}

function toCatalogLoad(
  query: {
    isError: boolean;
    data: { rows: SubjectRow[] } | undefined;
  },
  topic: string,
): SchemaPickerLoad {
  if (query.data != null) {
    const menu = projectMenu(query.data.rows, topic);
    return menu ? { kind: "ready", menu } : { kind: "empty" };
  }
  if (query.isError) return { kind: "empty" };
  return { kind: "pending" };
}

function useSchemaPickerView(input: {
  cluster: string;
  topic: string;
  value: number | null;
  page: readonly FramingRecord[];
}): SchemaPickerView {
  const intent = pickerIntent(input.value, input.page);
  const query = useSubjectRows(input.cluster, intent.kind === "active");
  return schemaPickerView(intent, toCatalogLoad(query, input.topic));
}

export function SchemaPicker({
  cluster,
  topic,
  value,
  page,
  onChange,
}: {
  cluster: string;
  topic: string;
  value: number | null;
  page: readonly FramingRecord[];
  onChange: (schemaId: number | null) => void;
}) {
  const view = useSchemaPickerView({ cluster, topic, value, page });
  const [open, setOpen] = useState(false);

  if (view.kind === "hidden") return null;

  if (view.kind === "pending") {
    return <SchemaPickerTrigger binding={view.binding} />;
  }

  function choose(id: number | null) {
    onChange(id);
    setOpen(false);
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger render={<Button {...TRIGGER_BUTTON} />}>
        <TriggerLabel trigger={view.trigger} />
        <ChevronsUpDownIcon className="shrink-0 text-muted-foreground" />
      </PopoverTrigger>
      <PopoverContent align="start" side="bottom" className="w-80 gap-0 p-0">
        <SchemaPickerMenu menu={view.menu} value={value} onChoose={choose} />
      </PopoverContent>
    </Popover>
  );
}

function SchemaPickerTrigger({ binding }: { binding: number | null }) {
  return (
    <Button {...TRIGGER_BUTTON} disabled aria-busy>
      {binding == null ? (
        <span className="min-w-0 truncate text-muted-foreground">Decode with schema…</span>
      ) : (
        <span className="min-w-0 truncate font-mono">{binding}</span>
      )}
      <ChevronsUpDownIcon className="shrink-0 text-muted-foreground" />
    </Button>
  );
}

function TriggerLabel({ trigger }: { trigger: SchemaTrigger }) {
  switch (trigger.kind) {
    case "prompt":
      return <span className="min-w-0 truncate text-muted-foreground">Decode with schema…</span>;
    case "choice":
      return (
        <>
          <span className="min-w-0 truncate font-mono">{trigger.choice.subject}</span>
          <span className="shrink-0 text-muted-foreground">
            {trigger.choice.type} · v{trigger.choice.latestVersion}
          </span>
        </>
      );
    case "unresolved":
      return <span className="min-w-0 truncate font-mono">schema {trigger.id}</span>;
  }
}

function SchemaPickerMenu({
  menu,
  value,
  onChoose,
}: {
  menu: SchemaMenu;
  value: number | null;
  onChoose: (id: number | null) => void;
}) {
  return (
    <Command>
      <CommandInput placeholder="Search subjects…" />
      <CommandList>
        <CommandEmpty>No matching subjects.</CommandEmpty>
        <CommandGroup>
          <CommandItem
            value="raw no schema"
            data-checked={value == null || undefined}
            onSelect={() => onChoose(null)}
          >
            Raw
          </CommandItem>
        </CommandGroup>
        {menu.pinned.length > 0 ? (
          <CommandGroup heading="Suggested">
            {menu.pinned.map((choice) => (
              <SubjectItem
                key={choice.subject}
                choice={choice}
                checked={choice.id === value}
                onSelect={() => onChoose(choice.id)}
              />
            ))}
          </CommandGroup>
        ) : null}
        {menu.rest.length > 0 ? (
          <CommandGroup heading={menu.pinned.length > 0 ? "All" : undefined}>
            {menu.rest.map((choice) => (
              <SubjectItem
                key={choice.subject}
                choice={choice}
                checked={choice.id === value}
                onSelect={() => onChoose(choice.id)}
              />
            ))}
          </CommandGroup>
        ) : null}
      </CommandList>
    </Command>
  );
}

function SubjectItem({
  choice,
  checked,
  onSelect,
}: {
  choice: SchemaChoice;
  checked: boolean;
  onSelect: () => void;
}) {
  return (
    <CommandItem
      value={`${choice.subject} ${choice.type} ${choice.id}`}
      data-checked={checked || undefined}
      onSelect={onSelect}
      className="min-w-0"
    >
      <span className="min-w-0 flex-1 truncate font-mono text-sm" title={choice.subject}>
        {choice.subject}
      </span>
      <span className="shrink-0 text-xs text-muted-foreground">
        {choice.type} · v{choice.latestVersion}
      </span>
    </CommandItem>
  );
}
