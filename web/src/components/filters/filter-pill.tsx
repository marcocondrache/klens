import { useState } from "react";
import { XIcon } from "lucide-react";

import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { FilterPanel } from "@/components/filters/filter-panel";
import { isEmptyFilterValue, summarizeFilter } from "@/lib/filters/state";
import type { FilterErrors, FilterFieldDef, FilterState, FilterValue } from "@/lib/filters/types";
import { cn } from "@/lib/utils";

export function FilterPill({
  fields,
  field,
  value,
  state,
  errors,
  onChange,
  onRemove,
}: {
  fields: FilterFieldDef[];
  field: FilterFieldDef;
  value: FilterValue;
  state: FilterState;
  errors?: FilterErrors;
  onChange: (id: string, value: FilterValue) => void;
  onRemove: () => void;
}) {
  const [open, setOpen] = useState(false);
  const detail = summarizeFilter(field, value);
  const invalid = Boolean(errors?.[field.id]);

  return (
    <div
      data-slot="filter-pill"
      className={cn(
        "inline-flex h-8 max-w-full items-center rounded-full border bg-card text-sm",
        isEmptyFilterValue(value) && "border-dashed",
        invalid && "border-destructive",
      )}
    >
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger
          render={
            <button
              type="button"
              className="flex h-full min-w-0 items-center gap-1.5 rounded-l-full pr-1.5 pl-2.5 hover:bg-muted"
            />
          }
        >
          <span className="shrink-0 text-muted-foreground">{field.label}</span>
          {detail ? <span className="min-w-0 truncate font-medium">{detail}</span> : null}
        </PopoverTrigger>
        <PopoverContent align="start" side="bottom" className="w-72 gap-0 p-0">
          <FilterPanel
            fields={fields}
            state={state}
            initialFieldId={field.id}
            errors={errors}
            onChange={onChange}
            onClose={() => setOpen(false)}
          />
        </PopoverContent>
      </Popover>
      <button
        type="button"
        onClick={onRemove}
        aria-label={`Remove ${field.label} filter`}
        className="flex h-full shrink-0 items-center rounded-r-full pr-2 pl-1 text-muted-foreground hover:bg-muted hover:text-foreground"
      >
        <XIcon className="size-3.5" />
      </button>
    </div>
  );
}
