import { useState } from "react";
import { CheckIcon, CopyIcon } from "lucide-react";
import { cn } from "@/lib/utils";

import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

export function CopyButton({
  value,
  label = "Copy",
  className,
  size = "icon-xs",
}: {
  value: string;
  label?: string;
  className?: string;
  size?: "icon-xs" | "icon-sm" | "icon";
}) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      return;
    }

    setCopied(true);
    setTimeout(() => setCopied(false), 1200);
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
            className={cn("text-muted-foreground hover:text-foreground", className)}
          />
        }
      >
        {copied ? <CheckIcon className="text-emerald-500" /> : <CopyIcon />}
      </TooltipTrigger>
      <TooltipContent>{copied ? "Copied" : label}</TooltipContent>
    </Tooltip>
  );
}
