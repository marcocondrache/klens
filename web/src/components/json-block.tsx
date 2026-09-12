import { useMemo } from "react";

import { highlightJsonHtml } from "@/lib/json-highlight";
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
  const html = useMemo(() => highlightJsonHtml(source), [source]);

  return (
    <div
      className={cn(
        "json-block overflow-auto rounded-lg border bg-muted/30 p-3 font-mono text-sm leading-relaxed",
        wrap && "json-block-wrap",
        className,
      )}
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
