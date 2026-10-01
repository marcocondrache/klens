import { cn } from "@/lib/utils";

function RefreshBar({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      role="status"
      aria-label="Refreshing"
      className={cn("pointer-events-none h-0.5 overflow-hidden", className)}
      {...props}
    >
      <div className="h-full w-1/3 animate-indeterminate bg-primary" />
    </div>
  );
}

export { RefreshBar };
