import { useMemo, useState, type ReactNode } from "react";
import { DownloadIcon, Maximize2Icon, Minimize2Icon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CopyButton } from "@/components/copy-button";
import { JsonBlock } from "@/components/json-block";
import { isJson, prettyJson } from "@/lib/format";
import { cn } from "@/lib/utils";

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
  actions,
}: {
  label: string;
  source: string;
  filename?: string;
  copyLabel?: string;
  showCopy?: boolean;
  showDownload?: boolean;
  showExpand?: boolean;
  expanded?: boolean;
  onExpandedChange?: (expanded: boolean) => void;
  fill?: boolean;
  actions?: ReactNode;
}) {
  const json = useMemo(() => isJson(source), [source]);
  const prettySource = useMemo(() => (json ? prettyJson(source) : source), [json, source]);
  const [pretty, setPretty] = useState(true);
  const displayed = json && pretty ? prettySource : source;
  const showPrettyToggle = json && prettySource !== source;

  function download() {
    const blob = new Blob([source], { type: json ? "application/json" : "text/plain" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename ?? (json ? "payload.json" : "payload.txt");
    anchor.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div
      className={cn("flex min-h-0 flex-col gap-2", fill ? "flex-1 overflow-hidden" : "shrink-0")}
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <h3 className="text-sm font-medium tracking-wide text-muted-foreground">{label}</h3>
          {actions}
        </div>
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
              pressed={expanded}
              onClick={() => onExpandedChange?.(!expanded)}
            >
              {expanded ? <Minimize2Icon /> : <Maximize2Icon />}
            </IconButton>
          ) : null}
        </div>
      </div>
      <JsonBlock source={displayed} wrap className={fill ? "min-h-0 flex-1" : "max-h-40"} />
    </div>
  );
}

function IconButton({
  label,
  onClick,
  pressed,
  children,
}: {
  label: string;
  onClick: () => void;
  pressed?: boolean;
  children: ReactNode;
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            variant="ghost"
            size="icon-xs"
            aria-label={label}
            aria-pressed={pressed}
            className="text-muted-foreground"
            onClick={onClick}
          />
        }
      >
        {children}
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}
