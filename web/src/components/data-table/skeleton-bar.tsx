import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

const WIDTHS = ["72%", "48%", "86%", "60%", "78%", "42%", "66%"];

export function SkeletonBar({
  row,
  column,
  align,
}: {
  row: number;
  column: number;
  align?: "left" | "right";
}) {
  return (
    <Skeleton
      className={cn("h-3 max-w-40 rounded-sm", align === "right" && "ml-auto")}
      style={{
        width: WIDTHS[(row * 3 + column * 5) % WIDTHS.length],
        animationDelay: `${row * 70}ms`,
      }}
    />
  );
}

export function skeletonRowStyle(row: number, count: number) {
  return { opacity: 1 - (row / count) * 0.8 };
}
