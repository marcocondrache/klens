import type { HTMLAttributes } from "react";
import type { Column, RowData } from "@tanstack/react-table";
import { ArrowDownIcon, ArrowUpIcon, ChevronsUpDownIcon, EyeIcon, EyeOffIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";

import { type DataTableFeatures } from "./features";

interface DataTableColumnHeaderProps<
  TData extends RowData,
  TValue,
> extends HTMLAttributes<HTMLDivElement> {
  column: Column<DataTableFeatures, TData, TValue>;
  title: string;
}

function columnLabel<TData extends RowData>(column: Column<DataTableFeatures, TData, unknown>) {
  return column.columnDef.meta?.label ?? column.id;
}

export function DataTableColumnHeader<TData extends RowData, TValue>({
  column,
  title,
  className,
}: DataTableColumnHeaderProps<TData, TValue>) {
  const canSort = column.getCanSort();
  const hidden = column.table
    .getAllLeafColumns()
    .filter((candidate) => candidate.getCanHide() && !candidate.getIsVisible());
  const allowHide = column.getCanHide() && column.table.getVisibleLeafColumns().length > 1;

  if (!canSort && !allowHide && hidden.length === 0) {
    return <div className={cn(className)}>{title}</div>;
  }

  const sorted = column.getIsSorted();

  return (
    <div className={cn("flex items-center gap-2", className)}>
      <DropdownMenu>
        <DropdownMenuTrigger render={<Button variant="ghost" size="sm" className="-ml-3" />}>
          <span>{title}</span>
          {canSort ? (
            sorted === "desc" ? (
              <ArrowDownIcon data-icon="inline-end" />
            ) : sorted === "asc" ? (
              <ArrowUpIcon data-icon="inline-end" />
            ) : (
              <ChevronsUpDownIcon data-icon="inline-end" />
            )
          ) : null}
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          {canSort ? (
            <DropdownMenuGroup>
              <DropdownMenuItem onClick={() => column.toggleSorting(false)}>
                <ArrowUpIcon />
                Asc
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => column.toggleSorting(true)}>
                <ArrowDownIcon />
                Desc
              </DropdownMenuItem>
            </DropdownMenuGroup>
          ) : null}
          {allowHide ? (
            <>
              {canSort ? <DropdownMenuSeparator /> : null}
              <DropdownMenuGroup>
                <DropdownMenuItem onClick={() => column.toggleVisibility(false)}>
                  <EyeOffIcon />
                  Hide
                </DropdownMenuItem>
              </DropdownMenuGroup>
            </>
          ) : null}
          {hidden.length > 0 ? (
            <>
              {canSort || allowHide ? <DropdownMenuSeparator /> : null}
              <DropdownMenuGroup>
                {hidden.map((candidate) => (
                  <DropdownMenuItem
                    key={candidate.id}
                    onClick={() => candidate.toggleVisibility(true)}
                  >
                    <EyeIcon />
                    Show {columnLabel(candidate)}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuGroup>
            </>
          ) : null}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
