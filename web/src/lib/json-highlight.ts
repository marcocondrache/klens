import { createHighlighter } from "@tanstack/highlight/core";
import { json } from "@tanstack/highlight/languages/json";
import { createHighlightedCodeBlockProps } from "@tanstack/highlight/react";

const jsonHighlighter = createHighlighter({ languages: [json] });

export function highlightJsonBlock(source: string) {
  return createHighlightedCodeBlockProps({
    highlighter: jsonHighlighter,
    code: source,
    lang: "json",
  });
}
