import { ChevronLeftIcon, ChevronRightIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface DataTablePagerProps {
  rowCount: number;
  pageSize: number;
  pageSizes: number[];
  hasMore?: boolean;
  canPreviousPage?: boolean;
  onPreviousPage?: () => void;
  onNextPage?: () => void;
  onPageSizeChange?: (pageSize: number) => void;
  loadingMore?: boolean;
}

export function DataTablePager({
  rowCount,
  pageSize,
  pageSizes,
  hasMore = false,
  canPreviousPage = false,
  onPreviousPage,
  onNextPage,
  onPageSizeChange,
  loadingMore = false,
}: DataTablePagerProps) {
  const sizes = pageSizes.includes(pageSize)
    ? pageSizes
    : [...pageSizes, pageSize].sort((left, right) => left - right);
  const pageSizeItems = sizes.map((size) => ({
    value: String(size),
    label: String(size),
  }));

  return (
    <div className="flex items-center justify-between px-2">
      <p className="flex-1 text-sm text-muted-foreground">
        {rowCount} {rowCount === 1 ? "record" : "records"}
      </p>
      <div className="flex items-center gap-6 lg:gap-8">
        {onPageSizeChange ? (
          <div className="flex items-center gap-2">
            <p className="text-sm font-medium">Limit</p>
            <Select
              value={String(pageSize)}
              items={pageSizeItems}
              onValueChange={(value) => onPageSizeChange(Number(value))}
            >
              <SelectTrigger className="w-[70px]">
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
        ) : null}
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="icon"
            onClick={onPreviousPage}
            disabled={!canPreviousPage}
          >
            <span className="sr-only">Load previous records</span>
            <ChevronLeftIcon />
          </Button>
          <Button
            variant="outline"
            size="icon"
            onClick={onNextPage}
            disabled={!hasMore || loadingMore}
          >
            <span className="sr-only">Load next records</span>
            <ChevronRightIcon />
          </Button>
        </div>
      </div>
    </div>
  );
}
