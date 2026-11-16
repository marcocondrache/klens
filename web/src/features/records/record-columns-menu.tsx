import type { ColumnVisibilityState } from "@tanstack/react-table";
import { Columns3Icon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

import { recordColumns } from "./record-columns";

const HIDEABLE = recordColumns.flatMap((column) => {
  const id = "accessorKey" in column ? column.accessorKey : column.id;
  if (id == null || column.enableHiding === false || typeof column.header !== "string") return [];

  return [{ id, label: column.header }];
});

export function RecordColumnsMenu({
  value,
  onChange,
}: {
  value: ColumnVisibilityState;
  onChange: (value: ColumnVisibilityState) => void;
}) {
  const hidden = HIDEABLE.filter((column) => value[column.id] === false).length;

  return (
    <DropdownMenu>
      <Tooltip>
        <TooltipTrigger
          render={
            <DropdownMenuTrigger
              render={
                <Button
                  variant="outline"
                  size="icon"
                  aria-label="Columns"
                  className="font-normal text-muted-foreground hover:text-foreground aria-expanded:text-foreground"
                />
              }
            />
          }
        >
          <Columns3Icon className="size-3.5" />
        </TooltipTrigger>
        <TooltipContent>{hidden > 0 ? `Columns (${hidden} hidden)` : "Columns"}</TooltipContent>
      </Tooltip>
      <DropdownMenuContent align="end" className="min-w-40">
        <DropdownMenuGroup>
          <DropdownMenuLabel className="text-xs text-muted-foreground">Columns</DropdownMenuLabel>
          {HIDEABLE.map((column) => (
            <DropdownMenuCheckboxItem
              key={column.id}
              checked={value[column.id] !== false}
              onCheckedChange={(checked) => onChange({ ...value, [column.id]: checked })}
            >
              {column.label}
            </DropdownMenuCheckboxItem>
          ))}
        </DropdownMenuGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
