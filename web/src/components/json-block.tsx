import { useMemo } from "react";

import { highlightJsonBlock } from "@/lib/json-highlight";
import { cn } from "@/lib/utils";

// The highlighter rescans to the end of the text from every unterminated `"`
// or `/*`, which only text that is not JSON holds, and its regexes overflow
// V8's stack on a string of a few MiB. A record a producer crafts could
// otherwise freeze or crash the tab.
const MAX_JSON_CHARS = 64 * 1024;
const MAX_TEXT_CHARS = 8 * 1024;

export function JsonBlock({
  source,
  json,
  className,
}: {
  source: string;
  json: boolean;
  className?: string;
}) {
  const { htmlMarkup } = useMemo(() => {
    const limit = json ? MAX_JSON_CHARS : MAX_TEXT_CHARS;
    return highlightJsonBlock(source, source.length <= limit ? "json" : "plaintext");
  }, [source, json]);

  return (
    <div
      className={cn(
        "json-block overflow-auto rounded-lg border bg-subtle px-3 py-2.5 font-mono text-sm leading-relaxed",
        className,
      )}
      dangerouslySetInnerHTML={{ __html: htmlMarkup }}
    />
  );
}
