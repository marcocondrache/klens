import { useMemo, useState, type ReactNode } from "react";
import { ArrowDownIcon, ArrowUpIcon, ChevronLeftIcon, ChevronRightIcon } from "lucide-react";
import { cn } from "@/lib/utils";

import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

export interface Column<T> {
  id: string;
  header: ReactNode;
  cell: (row: T) => ReactNode;
  sortValue?: (row: T) => string | number;
  align?: "left" | "right";
  className?: string;
  headerClassName?: string;
}

type Direction = "asc" | "desc";

function tablePlaceholder(content: ReactNode) {
  if (typeof content === "string") {
    return <p className="py-10 text-center text-sm text-muted-foreground">{content}</p>;
  }

  return content;
}

interface DataTableProps<T> {
  columns: Array<Column<T>>;
  rows: T[];
  rowKey: (row: T) => string;
  onRowClick?: (row: T) => void;
  selectedKey?: string;
  loading?: boolean;
  error?: ReactNode;
  emptyState?: ReactNode;
  defaultSort?: { id: string; direction: Direction };
  pageSize?: number;
  page?: number;
  hasMore?: boolean;
  onPageChange?: (page: number) => void;
}

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  onRowClick,
  selectedKey,
  loading = false,
  error,
  emptyState,
  defaultSort,
  pageSize = 25,
  page,
  hasMore = false,
  onPageChange,
}: DataTableProps<T>) {
  const [sort, setSort] = useState<{ id: string; direction: Direction } | null>(
    defaultSort ?? null,
  );
  const [localPage, setLocalPage] = useState(0);
  const serverPaging = onPageChange != null;

  const sorted = useMemo(() => {
    if (!sort) return rows;

    const column = columns.find((candidate) => candidate.id === sort.id);
    if (!column?.sortValue) return rows;

    const factor = sort.direction === "asc" ? 1 : -1;

    return [...rows].sort((left, right) => {
      const leftValue = column.sortValue!(left);
      const rightValue = column.sortValue!(right);

      if (typeof leftValue === "number" && typeof rightValue === "number") {
        return (leftValue - rightValue) * factor;
      }

      return String(leftValue).localeCompare(String(rightValue)) * factor;
    });
  }, [rows, sort, columns]);

  const pageCount = Math.max(1, Math.ceil(sorted.length / pageSize));
  const current = serverPaging ? (page ?? 0) : Math.min(localPage, pageCount - 1);
  const visible = serverPaging
    ? sorted
    : sorted.slice(current * pageSize, current * pageSize + pageSize);
  const showPager = serverPaging ? current > 0 || hasMore : pageCount > 1;

  function goToPage(next: number) {
    if (onPageChange) {
      onPageChange(next);
      return;
    }
    setLocalPage(next);
  }

  function toggleSort(column: Column<T>) {
    if (!column.sortValue) return;

    if (!serverPaging) setLocalPage(0);
    setSort((previous) => {
      if (previous?.id !== column.id) return { id: column.id, direction: "asc" };
      if (previous.direction === "asc") return { id: column.id, direction: "desc" };
      return null;
    });
  }

  return (
    <div className="space-y-3">
      <div className="overflow-hidden rounded-xl border bg-card">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              {columns.map((column) => (
                <TableHead
                  key={column.id}
                  className={cn(
                    "h-10 bg-muted/40 text-sm font-medium tracking-wide text-muted-foreground",
                    column.align === "right" && "text-right",
                    column.sortValue && "cursor-pointer select-none hover:text-foreground",
                    column.headerClassName,
                  )}
                  onClick={() => toggleSort(column)}
                >
                  <span
                    className={cn(
                      "inline-flex items-center gap-1",
                      column.align === "right" && "flex-row-reverse",
                    )}
                  >
                    {column.header}
                    {sort?.id === column.id ? (
                      sort.direction === "asc" ? (
                        <ArrowUpIcon className="size-3" />
                      ) : (
                        <ArrowDownIcon className="size-3" />
                      )
                    ) : null}
                  </span>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              Array.from({ length: 6 }, (_, index) => (
                <TableRow key={index} className="hover:bg-transparent">
                  {columns.map((column) => (
                    <TableCell key={column.id} className="py-2.5">
                      <Skeleton className="h-4 w-full max-w-32" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : visible.length === 0 ? (
              <TableRow className="hover:bg-transparent">
                <TableCell colSpan={columns.length} className="p-0">
                  {tablePlaceholder(error ?? emptyState ?? "No results.")}
                </TableCell>
              </TableRow>
            ) : (
              visible.map((row, index) => {
                const key = rowKey(row);
                const selected = selectedKey === key;

                return (
                  <TableRow
                    key={key}
                    data-state={selected ? "selected" : undefined}
                    onClick={onRowClick ? () => onRowClick(row) : undefined}
                    className={cn(
                      "border-border",
                      index % 2 === 1 && !selected && "bg-muted/35",
                      onRowClick && "cursor-pointer",
                    )}
                  >
                    {columns.map((column) => (
                      <TableCell
                        key={column.id}
                        className={cn(
                          "py-2.5 text-sm",
                          column.align === "right" && "text-right numeric",
                          column.className,
                        )}
                      >
                        {column.cell(row)}
                      </TableCell>
                    ))}
                  </TableRow>
                );
              })
            )}
          </TableBody>
        </Table>
      </div>

      {showPager ? (
        <div className="flex items-center justify-between px-1 text-sm text-muted-foreground">
          <span className="numeric">
            {current * pageSize + (visible.length > 0 ? 1 : 0)}
            {visible.length > 0 ? `–${current * pageSize + visible.length}` : ""}
            {serverPaging ? (hasMore ? "+" : "") : ` of ${sorted.length}`}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon-xs"
              disabled={current === 0}
              onClick={() => goToPage(current - 1)}
              aria-label="Previous page"
            >
              <ChevronLeftIcon />
            </Button>
            <span className="numeric px-2">
              {serverPaging ? `Page ${current + 1}` : `${current + 1} / ${pageCount}`}
            </span>
            <Button
              variant="outline"
              size="icon-xs"
              disabled={serverPaging ? !hasMore : current >= pageCount - 1}
              onClick={() => goToPage(current + 1)}
              aria-label="Next page"
            >
              <ChevronRightIcon />
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
