import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Checkbox } from "@/components/ui/checkbox";
import { EditorSearchHeader, type FilterEditorProps } from "@/components/filters/editors/shared";
import type { FilterOption } from "@/lib/filters/types";

/** The trailing check baked into `CommandItem` is redundant next to a checkbox. */
const NO_TRAILING_CHECK = "[&>svg]:hidden";

function searchValue(option: FilterOption) {
  return `${option.label} ${option.keywords ?? ""}`;
}

function toggle(values: string[], value: string) {
  return values.includes(value) ? values.filter((entry) => entry !== value) : [...values, value];
}

export function MultiSelectEditor({
  field,
  value,
  header,
  onChange,
}: FilterEditorProps<"multi-select">) {
  return (
    <Command loop>
      <EditorSearchHeader header={header} />
      <CommandList>
        <CommandEmpty>No matches.</CommandEmpty>
        <CommandGroup>
          <CommandItem
            value={field.anyLabel ?? `Any ${field.label}`}
            className={NO_TRAILING_CHECK}
            onSelect={() => onChange({ kind: "multi-select", values: [] })}
          >
            <span className="pl-6">{field.anyLabel ?? `Any ${field.label}`}</span>
          </CommandItem>
          {field.options.map((option) => (
            <CommandItem
              key={option.value}
              value={searchValue(option)}
              className={NO_TRAILING_CHECK}
              onSelect={() =>
                onChange({ kind: "multi-select", values: toggle(value.values, option.value) })
              }
            >
              <Checkbox
                checked={value.values.includes(option.value)}
                aria-hidden
                tabIndex={-1}
                className="pointer-events-none"
              />
              <span className="min-w-0 flex-1 truncate">{option.label}</span>
              {option.hint ? (
                <span className="shrink-0 text-xs text-muted-foreground">{option.hint}</span>
              ) : null}
            </CommandItem>
          ))}
        </CommandGroup>
      </CommandList>
    </Command>
  );
}

export function ChoiceEditor({
  field,
  value,
  header,
  onChange,
  onClose,
}: FilterEditorProps<"choice">) {
  function choose(next: string) {
    onChange({ kind: "choice", value: next });
    onClose();
  }

  return (
    <Command loop>
      <EditorSearchHeader header={header} />
      <CommandList>
        <CommandEmpty>No matches.</CommandEmpty>
        <CommandGroup>
          <CommandItem
            value={field.anyLabel ?? `Any ${field.label}`}
            data-checked={value.value === "" || undefined}
            onSelect={() => choose("")}
          >
            {field.anyLabel ?? `Any ${field.label}`}
          </CommandItem>
          {field.options.map((option) => (
            <CommandItem
              key={option.value}
              value={searchValue(option)}
              data-checked={option.value === value.value || undefined}
              onSelect={() => choose(option.value)}
            >
              <span className="min-w-0 flex-1 truncate">{option.label}</span>
              {option.hint ? (
                <span className="shrink-0 text-xs text-muted-foreground">{option.hint}</span>
              ) : null}
            </CommandItem>
          ))}
        </CommandGroup>
      </CommandList>
    </Command>
  );
}
