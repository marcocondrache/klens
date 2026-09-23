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
    <InputGroup className={cn("w-full max-w-72 bg-background dark:bg-input/20", className)}>
      <InputGroupAddon>
        <SearchIcon className="size-3.5!" />
      </InputGroupAddon>
      <InputGroupInput data-search-hotkey {...props} />
      <InputGroupAddon align="inline-end">
        <Kbd className="h-4.5 min-w-4.5 border bg-transparent text-[0.6875rem]">/</Kbd>
      </InputGroupAddon>
    </InputGroup>
  );
}
