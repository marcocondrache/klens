import { Children, Fragment, type ReactNode } from "react";

export function Facts({ children }: { children: ReactNode }) {
  const items = Children.toArray(children).filter(Boolean);

  return (
    <div className="numeric flex flex-wrap items-center gap-x-2 gap-y-1">
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
