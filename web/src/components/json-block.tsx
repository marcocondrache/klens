import { useMemo } from "react";

import { highlightJsonBlock } from "@/lib/json-highlight";
import { cn } from "@/lib/utils";

// The highlighter rescans to the end of the text from every unterminated
// `/*`, so a record a producer crafts could freeze the tab for minutes.
const MAX_HIGHLIGHTED_CHARS = 64 * 1024;

export function JsonBlock({ source, className }: { source: string; className?: string }) {
  const htmlMarkup = useMemo(
    () => (source.length > MAX_HIGHLIGHTED_CHARS ? null : highlightJsonBlock(source).htmlMarkup),
    [source],
  );
  const classes = cn(
    "json-block overflow-auto rounded-lg border bg-subtle px-3 py-2.5 font-mono text-sm leading-relaxed",
    className,
  );

  if (htmlMarkup === null) {
    return (
      <div className={classes}>
        <pre className="th-code">
          <code>{source}</code>
        </pre>
      </div>
    );
  }

  return <div className={classes} dangerouslySetInnerHTML={{ __html: htmlMarkup }} />;
}
