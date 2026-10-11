import { EyeOffIcon } from "lucide-react";

import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Pill } from "@/components/status";

export function ObfuscatedBadge() {
  return (
    <Tooltip>
      <TooltipTrigger render={<Pill tone="brand" className="cursor-default" />}>
        <EyeOffIcon className="size-3.5" />
        Obfuscated
      </TooltipTrigger>
      <TooltipContent className="block max-w-80 py-2 leading-relaxed">
        An obfuscation rule hides fields on this topic. Hidden fields show as *** or as a kx: token,
        and equal values get equal tokens. Values the schema registry could not decode are hidden
        whole. Search sees this view, not the original data.
      </TooltipContent>
    </Tooltip>
  );
}
