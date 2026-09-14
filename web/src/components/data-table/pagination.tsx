import type { ReactTable, RowData } from "@tanstack/react-table";
import {
  ChevronLeftIcon,
  ChevronRightIcon,
  ChevronsLeftIcon,
  ChevronsRightIcon,
} from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

import { type DataTableFeatures } from "./features";

export const PAGE_SIZES = [25, 50, 100, 200, 500];
export const DEFAULT_PAGE_SIZE = 100;

interface DataTablePaginationProps<TData extends RowData> {
  table: ReactTable<DataTableFeatures, TData>;
  manual?: boolean;
  hasMore?: boolean;
  canPreviousPage?: boolean;
  onPreviousPage?: () => void;
  onNextPage?: () => void;
  loadingMore?: boolean;
}

export function DataTablePagination<TData extends RowData>({
  table,
  manual = false,
  hasMore = false,
  canPreviousPage = false,
  onPreviousPage,
  onNextPage,
  loadingMore = false,
}: DataTablePaginationProps<TData>) {
  const pageSize = table.state.pagination.pageSize;
  const pageIndex = table.state.pagination.pageIndex;
  const selected = table.getFilteredSelectedRowModel().rows.length;
  const filtered = table.getFilteredRowModel().rows.length;
  const pageCount = Math.max(1, table.getPageCount());
  const sizes = PAGE_SIZES.includes(pageSize)
    ? PAGE_SIZES
    : [...PAGE_SIZES, pageSize].sort((left, right) => left - right);
  const pageSizeItems = sizes.map((size) => ({
    value: String(size),
    label: String(size),
  }));

  const previousDisabled = manual ? !canPreviousPage : !table.getCanPreviousPage();
  const nextDisabled = manual ? !hasMore || loadingMore : !table.getCanNextPage();

  return (
    <div className="flex items-center justify-between px-2">
      <div className="flex-1 text-sm text-muted-foreground">
        {selected} of {filtered} row(s) selected.
      </div>
      <div className="flex items-center gap-6 lg:gap-8">
        {manual ? null : (
          <div className="flex items-center gap-2">
            <p className="text-sm font-medium">Rows per page</p>
            <Select
              value={String(pageSize)}
              items={pageSizeItems}
              onValueChange={(value) => {
                table.setPageSize(Number(value));
              }}
            >
              <SelectTrigger size="sm" className="w-[70px]">
                <SelectValue placeholder={String(pageSize)} />
              </SelectTrigger>
              <SelectContent side="top">
                <SelectGroup>
                  {pageSizeItems.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>
        )}
        <div className="flex w-[100px] items-center justify-center text-sm font-medium">
          {manual ? `Page ${pageIndex + 1}` : `Page ${pageIndex + 1} of ${pageCount}`}
        </div>
        <div className="flex items-center gap-2">
          {manual ? null : (
            <Button
              variant="outline"
              size="icon"
              className="hidden lg:flex"
              onClick={() => table.setPageIndex(0)}
              disabled={previousDisabled}
            >
              <span className="sr-only">Go to first page</span>
              <ChevronsLeftIcon />
            </Button>
          )}
          <Button
            variant="outline"
            size="icon"
            onClick={manual ? onPreviousPage : () => table.previousPage()}
            disabled={previousDisabled}
          >
            <span className="sr-only">Go to previous page</span>
            <ChevronLeftIcon />
          </Button>
          <Button
            variant="outline"
            size="icon"
            onClick={manual ? onNextPage : () => table.nextPage()}
            disabled={nextDisabled}
          >
            <span className="sr-only">Go to next page</span>
            <ChevronRightIcon />
          </Button>
          {manual ? null : (
            <Button
              variant="outline"
              size="icon"
              className="hidden lg:flex"
              onClick={() => table.setPageIndex(table.getPageCount() - 1)}
              disabled={nextDisabled}
            >
              <span className="sr-only">Go to last page</span>
              <ChevronsRightIcon />
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}
