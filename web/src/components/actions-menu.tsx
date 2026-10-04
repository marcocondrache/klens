import { useState, type ReactNode } from "react";
import { EllipsisIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

export function ActionsMenu({
  label,
  size = "icon",
  children,
}: {
  label: string;
  size?: "icon" | "icon-xs";
  children: ReactNode;
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <Button
            variant={size === "icon" ? "outline" : "ghost"}
            size={size}
            aria-label={label}
            className="text-muted-foreground hover:text-foreground aria-expanded:text-foreground"
          />
        }
      >
        <EllipsisIcon />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-auto min-w-44">
        {children}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/** One open dialog at a time, for actions a menu opens. */
export function useOpenDialog<Name extends string>() {
  const [open, setOpen] = useState<Name | null>(null);

  return {
    show: (name: Name) => setOpen(name),
    props: (name: Name) => ({
      open: open === name,
      onOpenChange: (next: boolean) => setOpen(next ? name : null),
    }),
  };
}
