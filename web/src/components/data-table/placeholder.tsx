import type { ReactNode } from "react";

/** Empty or error content for a table body. Strings get the standard muted line. */
export function tablePlaceholder(content: ReactNode) {
  if (typeof content === "string") {
    return <p className="py-10 text-center text-sm text-muted-foreground">{content}</p>;
  }

  return content;
}
