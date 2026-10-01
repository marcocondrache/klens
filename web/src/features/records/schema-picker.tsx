import { useMemo } from "react";
import { Combobox as ComboboxPrimitive } from "@base-ui/react";
import { ChevronsUpDownIcon, SearchIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Combobox,
  ComboboxCollection,
  ComboboxContent,
  ComboboxEmpty,
  ComboboxGroup,
  ComboboxItem,
  ComboboxLabel,
  ComboboxList,
} from "@/components/ui/combobox";
import { InputGroup, InputGroupAddon } from "@/components/ui/input-group";
import { useSubjectRows } from "@/lib/api/catalog";
import type { SubjectRow } from "@/lib/api/types";
import { cn } from "@/lib/utils";

type SchemaOption = { id: number | null; label: string; detail: string | null };

type SchemaGroup = { value: string; label: string | null; items: SchemaOption[] };

const RAW: SchemaOption = { id: null, label: "Raw", detail: null };

function subjectOption(row: SubjectRow): SchemaOption {
  return { id: row.id, label: row.subject, detail: `${row.type} · v${row.latestVersion}` };
}

export function SchemaPicker({
  cluster,
  topic,
  value,
  onChange,
}: {
  cluster: string;
  topic: string;
  value: number | null;
  onChange: (id: number | null) => void;
}) {
  const { data: subjects } = useSubjectRows(cluster);
  const preferred = `${topic}-value`;
  const options = useMemo(() => {
    const rows = subjects?.rows ?? [];
    return {
      pinned: rows.filter((subject) => subject.subject === preferred).map(subjectOption),
      rest: rows.filter((subject) => subject.subject !== preferred).map(subjectOption),
    };
  }, [preferred, subjects]);
  const all = [...options.pinned, ...options.rest];
  const selected = all.find((option) => option.id === value) ?? null;

  if (all.length === 0) {
    return null;
  }

  const groups: SchemaGroup[] = [{ value: "raw", label: null, items: [RAW] }];
  if (options.pinned.length > 0) {
    groups.push({ value: "suggested", label: "Suggested", items: options.pinned });
  }
  if (options.rest.length > 0) {
    groups.push({
      value: "all",
      label: options.pinned.length > 0 ? "All" : null,
      items: options.rest,
    });
  }

  return (
    <Combobox
      items={groups}
      autoHighlight
      value={selected ?? RAW}
      onValueChange={(option) => {
        if (option) onChange(option.id);
      }}
      isItemEqualToValue={(item, current) => item.id === current.id}
      itemToStringLabel={(option) => option.label}
    >
      <ComboboxPrimitive.Trigger
        render={
          <Button
            variant="outline"
            className="max-w-64 min-w-0 gap-1 font-normal"
            aria-label="Decode value with schema"
          />
        }
      >
        <span className={cn("min-w-0 truncate", selected ? "font-mono" : "text-muted-foreground")}>
          {selected ? selected.label : "Decode with schema…"}
        </span>
        {selected ? (
          <span className="shrink-0 text-muted-foreground">{selected.detail}</span>
        ) : null}
        <ChevronsUpDownIcon className="shrink-0 text-muted-foreground" />
      </ComboboxPrimitive.Trigger>
      <ComboboxContent className="w-80 p-1">
        <div className="p-1 pb-0">
          <InputGroup className="h-8 border-input/30 bg-input/30 shadow-none">
            <ComboboxPrimitive.Input
              className="w-full text-sm outline-hidden"
              placeholder="Search subjects…"
            />
            <InputGroupAddon className="pl-2">
              <SearchIcon className="size-4 shrink-0 opacity-50" />
            </InputGroupAddon>
          </InputGroup>
        </div>
        <ComboboxEmpty>No matching subjects.</ComboboxEmpty>
        <ComboboxList className="p-0">
          {(group: SchemaGroup) => (
            <ComboboxGroup key={group.value} items={group.items} className="p-1">
              {group.label ? (
                <ComboboxLabel className="font-medium">{group.label}</ComboboxLabel>
              ) : null}
              <ComboboxCollection>
                {(option: SchemaOption) => (
                  <ComboboxItem
                    key={`${option.label}:${option.id}`}
                    value={option}
                    className="min-w-0 rounded-sm px-2 py-1.5 pr-8"
                  >
                    <span
                      className={cn("min-w-0 flex-1 truncate", option.id != null && "font-mono")}
                      title={option.id != null ? option.label : undefined}
                    >
                      {option.label}
                    </span>
                    {option.detail ? (
                      <span className="shrink-0 text-xs text-muted-foreground">
                        {option.detail}
                      </span>
                    ) : null}
                  </ComboboxItem>
                )}
              </ComboboxCollection>
            </ComboboxGroup>
          )}
        </ComboboxList>
      </ComboboxContent>
    </Combobox>
  );
}
