import { useRef, useState, type MouseEvent } from "react";
import { CheckIcon, CopyIcon } from "lucide-react";
import { cn } from "@/lib/utils";

import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

const ICON = "col-start-1 row-start-1 transition-[opacity,scale,filter] duration-200 ease-out";

export function CopyButton({
  value,
  label = "Copy",
  className,
  size = "icon-xs",
  reveal = false,
}: {
  value: string;
  label?: string;
  className?: string;
  size?: "icon-xs" | "icon-sm" | "icon";
  reveal?: boolean;
}) {
  const [copied, setCopied] = useState(false);
  const timeout = useRef<ReturnType<typeof setTimeout>>(undefined);

  async function copy(event: MouseEvent) {
    event.stopPropagation();
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      return;
    }

    clearTimeout(timeout.current);
    setCopied(true);
    timeout.current = setTimeout(() => setCopied(false), 1200);
  }

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            variant="ghost"
            size={size}
            onClick={copy}
            aria-label={label}
            className={cn(
              "text-muted-foreground hover:text-foreground",
              reveal &&
                "opacity-0 group-hover/row:opacity-100 focus-visible:opacity-100 data-popup-open:opacity-100",
              copied && "opacity-100",
              className,
            )}
          />
        }
      >
        <span className="grid">
          <CopyIcon className={cn(ICON, copied && "scale-50 opacity-0 blur-[2px]")} />
          <CheckIcon className={cn(ICON, "text-ok", !copied && "scale-50 opacity-0 blur-[2px]")} />
        </span>
      </TooltipTrigger>
      <TooltipContent>{copied ? "Copied" : label}</TooltipContent>
    </Tooltip>
  );
}
