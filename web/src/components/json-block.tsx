import { Fragment, useMemo } from "react"
import { cn } from "@/lib/utils"

const TOKEN =
  /("(?:\\.|[^"\\])*")(\s*:)|("(?:\\.|[^"\\])*")|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|\b(true|false|null)\b/g

interface Token {
  text: string
  className?: string
}

function tokenize(source: string): Token[] {
  const tokens: Token[] = []
  let cursor = 0

  for (const match of source.matchAll(TOKEN)) {
    const index = match.index ?? 0

    if (index > cursor) {
      tokens.push({ text: source.slice(cursor, index) })
    }

    const [full, key, colon, string, number, literal] = match

    if (key) {
      tokens.push({ text: key, className: "text-brand" })
      tokens.push({ text: colon ?? "" })
    } else if (string) {
      tokens.push({ text: string, className: "text-emerald-600 dark:text-emerald-400" })
    } else if (number) {
      tokens.push({ text: number, className: "text-sky-600 dark:text-sky-400" })
    } else if (literal) {
      tokens.push({ text: literal, className: "text-amber-600 dark:text-amber-400" })
    }

    cursor = index + full.length
  }

  if (cursor < source.length) {
    tokens.push({ text: source.slice(cursor) })
  }

  return tokens
}

export function JsonBlock({
  source,
  className,
  wrap = false,
}: {
  source: string
  className?: string
  wrap?: boolean
}) {
  const tokens = useMemo(() => tokenize(source), [source])

  return (
    <pre
      className={cn(
        "overflow-auto rounded-lg border bg-muted/30 p-3 font-mono text-xs leading-relaxed",
        wrap && "whitespace-pre-wrap break-all",
        className,
      )}
    >
      <code>
        {tokens.map((token, index) => (
          <Fragment key={index}>
            {token.className ? <span className={token.className}>{token.text}</span> : token.text}
          </Fragment>
        ))}
      </code>
    </pre>
  )
}
