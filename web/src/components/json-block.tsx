import { useMemo } from "react";

import { highlightJsonBlock } from "@/lib/json-highlight";
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
  const { htmlMarkup } = useMemo(() => highlightJsonBlock(source), [source]);

  return (
    <div
      className={cn(
        "json-block overflow-auto rounded-lg border bg-subtle px-3 py-2.5 font-mono text-sm leading-relaxed",
        wrap && "json-block-wrap",
        className,
      )}
      dangerouslySetInnerHTML={{ __html: htmlMarkup }}
    />
  );
}
