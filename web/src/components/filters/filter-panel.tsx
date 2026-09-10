import { useState } from "react";
import { ChevronDownIcon, ChevronRightIcon } from "lucide-react";

import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "@/components/ui/command";
import { DateRangeEditor } from "@/components/filters/editors/date-range";
import { KeyValueEditor } from "@/components/filters/editors/key-value";
import { NumberEditor, NumberRangeEditor } from "@/components/filters/editors/number";
import { RawEditor } from "@/components/filters/editors/raw";
import { ChoiceEditor, MultiSelectEditor } from "@/components/filters/editors/select";
import { TextEditor } from "@/components/filters/editors/text";
import { filterValue } from "@/lib/filters/state";
import type { FilterErrors, FilterFieldDef, FilterState, FilterValue } from "@/lib/filters/types";

/**
 * Popover body for the filter system. Starts on the field list, or directly on
 * one field's editor, and lets you switch fields from the editor header.
 */
export function FilterPanel({
  fields,
  state,
  initialFieldId = null,
  errors,
  onChange,
  onClose,
}: {
  fields: FilterFieldDef[];
  state: FilterState;
  initialFieldId?: string | null;
  errors?: FilterErrors;
  onChange: (id: string, value: FilterValue) => void;
  onClose: () => void;
}) {
  const [fieldId, setFieldId] = useState(initialFieldId);
  const field = fields.find((candidate) => candidate.id === fieldId);

  if (!field) {
    return <FieldPicker fields={fields} onSelect={setFieldId} />;
  }

  const header = <FieldSwitcher field={field} onBack={() => setFieldId(null)} />;
  const error = errors?.[field.id];
  const shared = { header, error, onClose } as const;

  switch (field.kind) {
    case "multi-select":
      return (
        <MultiSelectEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
    case "choice":
      return (
        <ChoiceEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
    case "text":
      return (
        <TextEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
    case "date-range":
      return (
        <DateRangeEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
    case "number":
      return (
        <NumberEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
    case "number-range":
      return (
        <NumberRangeEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
    case "key-value":
      return (
        <KeyValueEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
    case "raw":
      return (
        <RawEditor
          {...shared}
          field={field}
          value={filterValue(state, field)}
          onChange={(value) => onChange(field.id, value)}
        />
      );
  }
}

function FieldPicker({
  fields,
  onSelect,
}: {
  fields: FilterFieldDef[];
  onSelect: (id: string) => void;
}) {
  return (
    <Command loop>
      <CommandInput placeholder="Filter by…" />
      <CommandList>
        <CommandEmpty>No matches.</CommandEmpty>
        <CommandGroup>
          {fields.map((field) => (
            <CommandItem
              key={field.id}
              value={`${field.label} ${field.keywords ?? ""}`}
              onSelect={() => onSelect(field.id)}
            >
              {field.icon ? <field.icon className="text-muted-foreground" /> : null}
              <span className="min-w-0 flex-1 truncate">{field.label}</span>
              <CommandShortcut>
                <ChevronRightIcon className="size-4" />
              </CommandShortcut>
            </CommandItem>
          ))}
        </CommandGroup>
      </CommandList>
    </Command>
  );
}

function FieldSwitcher({ field, onBack }: { field: FilterFieldDef; onBack: () => void }) {
  return (
    <button
      type="button"
      onClick={onBack}
      className="inline-flex shrink-0 items-center gap-1 rounded-md px-1.5 py-1 text-sm font-medium hover:bg-muted"
    >
      {field.icon ? <field.icon className="size-4 text-muted-foreground" /> : null}
      {field.label}
      <ChevronDownIcon className="size-3.5 text-muted-foreground" />
    </button>
  );
}
