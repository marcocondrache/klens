import { lazy, Suspense, useMemo, useState, type ReactNode } from "react"
import { DownloadIcon, Maximize2Icon, Minimize2Icon } from "lucide-react"
import { useTheme } from "next-themes"

import type { FileContents } from "@pierre/diffs/react"

import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { CopyButton } from "@/components/copy-button"
import { isJson, prettyJson } from "@/lib/format"
import { cn } from "@/lib/utils"

const PierreFile = lazy(async () => {
  const { File } = await import("@pierre/diffs/react")
  return { default: File }
})

export function PayloadView({
  label,
  source,
  filename,
  copyLabel = "Copy",
  showCopy = true,
  showDownload = false,
  showExpand = false,
  expanded = false,
  onExpandedChange,
  fill = false,
}: {
  label: string
  source: string
  filename?: string
  copyLabel?: string
  showCopy?: boolean
  showDownload?: boolean
  showExpand?: boolean
  expanded?: boolean
  onExpandedChange?: (expanded: boolean) => void
  fill?: boolean
}) {
  const json = useMemo(() => isJson(source), [source])
  const prettySource = useMemo(() => (json ? prettyJson(source) : source), [json, source])
  const [pretty, setPretty] = useState(true)
  const displayed = json && pretty ? prettySource : source
  const showPrettyToggle = json && prettySource !== source
  const downloadName = filename ?? (json ? "payload.json" : "payload.txt")

  function download() {
    const blob = new Blob([source], { type: json ? "application/json" : "text/plain" })
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement("a")
    anchor.href = url
    anchor.download = downloadName
    anchor.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className={cn("flex min-h-0 flex-col gap-2", fill ? "flex-1 overflow-hidden" : "shrink-0")}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">{label}</h3>
        <div className="flex items-center gap-1">
          {showPrettyToggle ? (
            <div className="mr-1 flex rounded-lg border p-0.5">
              <Button
                variant={pretty ? "secondary" : "ghost"}
                size="xs"
                aria-pressed={pretty}
                onClick={() => setPretty(true)}
              >
                Pretty
              </Button>
              <Button
                variant={!pretty ? "secondary" : "ghost"}
                size="xs"
                aria-pressed={!pretty}
                onClick={() => setPretty(false)}
              >
                Raw
              </Button>
            </div>
          ) : null}
          {showCopy ? <CopyButton value={displayed} label={copyLabel} /> : null}
          {showDownload ? (
            <IconButton label="Download value" onClick={download}>
              <DownloadIcon />
            </IconButton>
          ) : null}
          {showExpand ? (
            <IconButton
              label={expanded ? "Collapse value" : "Expand value"}
              onClick={() => onExpandedChange?.(!expanded)}
            >
              {expanded ? <Minimize2Icon /> : <Maximize2Icon />}
            </IconButton>
          ) : null}
        </div>
      </div>
      <PayloadCode
        name={downloadName}
        contents={displayed}
        json={json}
        cacheKey={`${label}:${pretty ? "pretty" : "raw"}:${displayed.length}`}
        fill={fill}
      />
    </div>
  )
}

function PayloadCode({
  name,
  contents,
  json,
  cacheKey,
  fill,
}: {
  name: string
  contents: string
  json: boolean
  cacheKey: string
  fill: boolean
}) {
  const { resolvedTheme } = useTheme()
  const themeType: "light" | "dark" = resolvedTheme === "light" ? "light" : "dark"
  const file = useMemo<FileContents>(
    () => ({
      name,
      contents,
      lang: json ? "json" : "text",
      cacheKey,
    }),
    [cacheKey, contents, json, name],
  )
  const options = useMemo(
    () => ({
      theme: { dark: "pierre-dark" as const, light: "pierre-light" as const },
      themeType,
      overflow: "wrap" as const,
      disableFileHeader: true,
      disableLineNumbers: !fill,
      tokenizeMaxLineLength: 2000,
    }),
    [fill, themeType],
  )

  return (
    <Suspense
      fallback={<div className={cn("rounded-lg border bg-muted/30", fill ? "min-h-0 flex-1" : "h-24")} />}
    >
      <PierreFile
        file={file}
        options={options}
        disableWorkerPool
        className={cn("overflow-auto rounded-lg border", fill ? "min-h-0 flex-1" : "max-h-40")}
      />
    </Suspense>
  )
}

function IconButton({
  label,
  onClick,
  children,
}: {
  label: string
  onClick: () => void
  children: ReactNode
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            variant="ghost"
            size="icon-xs"
            aria-label={label}
            className="text-muted-foreground"
            onClick={onClick}
          />
        }
      >
        {children}
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  )
}
