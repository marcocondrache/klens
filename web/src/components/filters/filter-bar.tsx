import { useState } from "react";
import { ListFilterIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { FilterPanel } from "@/components/filters/filter-panel";
import { FilterPill } from "@/components/filters/filter-pill";
import { activeFilters } from "@/lib/filters/state";
import type { FilterErrors, FilterFieldDef, FilterState, FilterValue } from "@/lib/filters/types";
import { cn } from "@/lib/utils";

export function FilterBar({
  fields,
  state,
  errors,
  onChange,
  onRemove,
  onClear,
  className,
}: {
  fields: FilterFieldDef[];
  state: FilterState;
  errors?: FilterErrors;
  onChange: (id: string, value: FilterValue) => void;
  onRemove: (id: string) => void;
  onClear: () => void;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const active = activeFilters(fields, state);

  return (
    <div className={cn("flex flex-wrap items-center gap-2", className)}>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger
          render={
            <Button
              variant="outline"
              size="sm"
              className="rounded-full border-dashed font-normal"
            />
          }
        >
          <ListFilterIcon className="text-muted-foreground" />
          Add Filter
        </PopoverTrigger>
        <PopoverContent align="start" side="bottom" className="w-72 gap-0 p-0">
          <FilterPanel
            fields={fields}
            state={state}
            errors={errors}
            onChange={onChange}
            onClose={() => setOpen(false)}
          />
        </PopoverContent>
      </Popover>

      {active.map(({ field, value }) => (
        <FilterPill
          key={field.id}
          fields={fields}
          field={field}
          value={value}
          state={state}
          errors={errors}
          onChange={onChange}
          onRemove={() => onRemove(field.id)}
        />
      ))}

      {active.length > 0 ? (
        <Button variant="ghost" size="sm" className="font-normal" onClick={onClear}>
          Clear
        </Button>
      ) : null}
    </div>
  );
}
