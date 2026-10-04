import type { ReactNode } from "react";

import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

export function Field({
  label,
  htmlFor,
  action,
  hint,
  className,
  children,
}: {
  label: ReactNode;
  htmlFor?: string;
  action?: ReactNode;
  hint?: ReactNode;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div className={cn("grid min-w-0 gap-1.5", className)}>
      <div className="flex min-h-6 items-center justify-between gap-2">
        <Label htmlFor={htmlFor} className="text-xs text-muted-foreground">
          {label}
        </Label>
        {action}
      </div>
      {children}
      {hint ? (
        <p id={htmlFor && `${htmlFor}-hint`} className="text-xs text-muted-foreground">
          {hint}
        </p>
      ) : null}
    </div>
  );
}

export function FieldCount({ value }: { value: number }) {
  return <span className="numeric font-normal text-muted-foreground/60">{value}</span>;
}
