import { MoonIcon, SunIcon } from "lucide-react";
import { useTheme } from "next-themes";

import { IconButton } from "@/components/icon-button";

export function ModeToggle() {
  const { resolvedTheme, setTheme } = useTheme();
  const dark = resolvedTheme !== "light";

  return (
    <IconButton
      label="Toggle theme"
      tooltip={dark ? "Light mode" : "Dark mode"}
      size="icon-sm"
      onClick={() => setTheme(dark ? "light" : "dark")}
    >
      {dark ? <MoonIcon /> : <SunIcon />}
    </IconButton>
  );
}
