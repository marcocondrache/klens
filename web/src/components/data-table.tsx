import { useMemo, useState, type ReactNode } from "react"
import { ArrowDownIcon, ArrowUpIcon, ChevronLeftIcon, ChevronRightIcon } from "lucide-react"
import { cn } from "@/lib/utils"

import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

export interface Column<T> {
  id: string
  header: ReactNode
  cell: (row: T) => ReactNode
  sortValue?: (row: T) => string | number
  align?: "left" | "right"
  className?: string
  headerClassName?: string
}

type Direction = "asc" | "desc"

interface DataTableProps<T> {
  columns: Array<Column<T>>
  rows: T[]
  rowKey: (row: T) => string
  onRowClick?: (row: T) => void
  loading?: boolean
  emptyState?: ReactNode
  defaultSort?: { id: string; direction: Direction }
  pageSize?: number
}

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  onRowClick,
  loading = false,
  emptyState,
  defaultSort,
  pageSize = 25,
}: DataTableProps<T>) {
  const [sort, setSort] = useState<{ id: string; direction: Direction } | null>(defaultSort ?? null)
  const [page, setPage] = useState(0)

  const sorted = useMemo(() => {
    if (!sort) return rows

    const column = columns.find((candidate) => candidate.id === sort.id)
    if (!column?.sortValue) return rows

    const factor = sort.direction === "asc" ? 1 : -1

    return [...rows].sort((left, right) => {
      const leftValue = column.sortValue!(left)
      const rightValue = column.sortValue!(right)

      if (typeof leftValue === "number" && typeof rightValue === "number") {
        return (leftValue - rightValue) * factor
      }

      return String(leftValue).localeCompare(String(rightValue)) * factor
    })
  }, [rows, sort, columns])

  const pageCount = Math.max(1, Math.ceil(sorted.length / pageSize))
  const current = Math.min(page, pageCount - 1)
  const visible = sorted.slice(current * pageSize, current * pageSize + pageSize)

  function toggleSort(column: Column<T>) {
    if (!column.sortValue) return

    setPage(0)
    setSort((previous) => {
      if (previous?.id !== column.id) return { id: column.id, direction: "asc" }
      if (previous.direction === "asc") return { id: column.id, direction: "desc" }
      return null
    })
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
                    "h-9 bg-muted/40 text-[0.7rem] font-medium tracking-wider text-muted-foreground uppercase",
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
                  {emptyState ?? (
                    <p className="py-10 text-center text-sm text-muted-foreground">No results.</p>
                  )}
                </TableCell>
              </TableRow>
            ) : (
              visible.map((row) => (
                <TableRow
                  key={rowKey(row)}
                  onClick={onRowClick ? () => onRowClick(row) : undefined}
                  className={cn(
                    "border-border/60",
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
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {pageCount > 1 ? (
        <div className="flex items-center justify-between px-1 text-xs text-muted-foreground">
          <span className="numeric">
            {current * pageSize + 1}–{Math.min(sorted.length, (current + 1) * pageSize)} of{" "}
            {sorted.length}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon-xs"
              disabled={current === 0}
              onClick={() => setPage(current - 1)}
              aria-label="Previous page"
            >
              <ChevronLeftIcon />
            </Button>
            <span className="numeric px-2">
              {current + 1} / {pageCount}
            </span>
            <Button
              variant="outline"
              size="icon-xs"
              disabled={current >= pageCount - 1}
              onClick={() => setPage(current + 1)}
              aria-label="Next page"
            >
              <ChevronRightIcon />
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  )
}
