import { Skeleton } from "@/components/ui/skeleton";

export function TabCount({ value }: { value: number | null | undefined }) {
  if (value == null) {
    return <Skeleton aria-hidden className="ml-1.5 h-3 w-4 rounded-sm" />;
  }

  return <span className="numeric ml-1.5 text-muted-foreground">{value}</span>;
}
