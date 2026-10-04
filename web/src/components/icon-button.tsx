import type { ComponentProps, ReactNode } from "react";

import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

export const REVEAL_ON_ROW =
  "opacity-0 group-hover/row:opacity-100 focus-visible:opacity-100 data-popup-open:opacity-100";

/**
 * Ghost icon button with a tooltip. `label` is the accessible name and the default tooltip.
 * `reveal` hides it until its table row is hovered.
 */
export function IconButton({
  label,
  tooltip = label,
  size = "icon-xs",
  reveal = false,
  className,
  children,
  ...props
}: Omit<ComponentProps<typeof Button>, "variant" | "size"> & {
  label: string;
  tooltip?: ReactNode;
  size?: "icon-xs" | "icon-sm" | "icon";
  reveal?: boolean;
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            variant="ghost"
            size={size}
            aria-label={label}
            className={cn("text-muted-foreground", reveal && REVEAL_ON_ROW, className)}
            {...props}
          />
        }
      >
        {children}
      </TooltipTrigger>
      <TooltipContent>{tooltip}</TooltipContent>
    </Tooltip>
  );
}
