import { Children, Fragment, type ReactNode } from "react";

import { cn } from "@/lib/utils";

export function Facts({ children, className }: { children: ReactNode; className?: string }) {
  const items = Children.toArray(children).filter(Boolean);

  return (
    <div className={cn("numeric flex flex-wrap items-center gap-x-2 gap-y-1", className)}>
      {items.map((item, index) => (
        <Fragment key={index}>
          {index > 0 ? (
            <span aria-hidden className="text-muted-foreground/40">
              ·
            </span>
          ) : null}
          {item}
        </Fragment>
      ))}
    </div>
  );
}
