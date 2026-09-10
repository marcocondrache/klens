import type { ComponentProps } from "react";
import { SearchIcon } from "lucide-react";

import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import { Kbd } from "@/components/ui/kbd";
import { cn } from "@/lib/utils";

export function SearchField({
  className,
  ...props
}: ComponentProps<typeof InputGroupInput> & {
  className?: string;
}) {
  return (
    <InputGroup className={cn("w-full max-w-sm", className)}>
      <InputGroupAddon>
        <SearchIcon />
      </InputGroupAddon>
      <InputGroupInput data-search-hotkey {...props} />
      <InputGroupAddon align="inline-end">
        <Kbd>/</Kbd>
      </InputGroupAddon>
    </InputGroup>
  );
}
