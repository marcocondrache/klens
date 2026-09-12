import { Fragment, useMemo } from "react";

import { tokenizeJson } from "@/lib/json-highlight";
import { cn } from "@/lib/utils";

export function JsonBlock({
  source,
  className,
  wrap = false,
}: {
  source: string;
  className?: string;
  wrap?: boolean;
}) {
  const tokens = useMemo(() => tokenizeJson(source), [source]);

  return (
    <pre
      className={cn(
        "json-block overflow-auto rounded-lg border bg-muted/30 p-3 font-mono text-sm leading-relaxed",
        wrap && "whitespace-pre-wrap break-words",
        className,
      )}
    >
      <code>
        {tokens.map((token, index) =>
          token.className ? (
            <span key={index} className={`th-token th-${token.className}`}>
              {token.value}
            </span>
          ) : (
            <Fragment key={index}>{token.value}</Fragment>
          ),
        )}
      </code>
    </pre>
  );
}
