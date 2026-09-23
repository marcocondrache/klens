import type { HTMLAttributes } from "react";
import type { Column, RowData } from "@tanstack/react-table";
import { ArrowDownIcon, ArrowUpIcon, ChevronsUpDownIcon } from "lucide-react";

import { cn } from "@/lib/utils";

import { type DataTableFeatures } from "./features";

interface DataTableColumnHeaderProps<
  TData extends RowData,
  TValue,
> extends HTMLAttributes<HTMLDivElement> {
  column: Column<DataTableFeatures, TData, TValue>;
  title: string;
}

/** Click to sort; the arrow only shows once the column is sorted, or on hover. */
export function DataTableColumnHeader<TData extends RowData, TValue>({
  column,
  title,
  className,
}: DataTableColumnHeaderProps<TData, TValue>) {
  const right = column.columnDef.meta?.align === "right";

  if (!column.getCanSort()) {
    return <div className={cn(right && "text-right", className)}>{title}</div>;
  }

  const sorted = column.getIsSorted();
  const Icon =
    sorted === "desc" ? ArrowDownIcon : sorted === "asc" ? ArrowUpIcon : ChevronsUpDownIcon;

  return (
    <div className={cn("flex", right && "justify-end", className)}>
      <button
        type="button"
        onClick={() => column.toggleSorting(sorted === "asc")}
        aria-label={`Sort by ${title}`}
        className={cn(
          "group/sort -mx-1 inline-flex h-6 items-center gap-1 rounded-md px-1 outline-none transition-colors hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/50",
          right && "flex-row-reverse",
          sorted && "text-foreground",
        )}
      >
        <span>{title}</span>
        <Icon
          aria-hidden
          className={cn(
            "size-3 shrink-0 transition-opacity",
            sorted ? "opacity-100" : "opacity-0 group-hover/sort:opacity-60",
          )}
        />
      </button>
    </div>
  );
}
