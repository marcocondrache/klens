import { Skeleton } from "@/components/ui/skeleton";

const SCHEMA_SKELETON: Array<[indent: number, width: number]> = [
  [0, 4],
  [1, 38],
  [1, 52],
  [1, 30],
  [1, 18],
  [2, 4],
  [3, 46],
  [3, 58],
  [2, 12],
  [3, 40],
  [3, 64],
  [2, 6],
  [1, 4],
  [0, 4],
];

export function SchemaLoading() {
  return (
    <div className="flex min-h-0 flex-1 flex-col gap-2" role="status" aria-live="polite">
      <div className="flex h-7 items-center">
        <Skeleton className="h-3 w-12 rounded-sm" />
      </div>
      <div
        aria-hidden
        className="min-h-0 flex-1 space-y-3 overflow-hidden rounded-lg border bg-subtle px-3 py-3.5"
      >
        {SCHEMA_SKELETON.map(([indent, width], index) => (
          <Skeleton
            key={index}
            className="h-3 rounded-sm"
            style={{
              marginLeft: `${indent * 1.25}rem`,
              width: `${width}%`,
              animationDelay: `${index * 50}ms`,
            }}
          />
        ))}
      </div>
      <span className="sr-only">Loading schema…</span>
    </div>
  );
}
