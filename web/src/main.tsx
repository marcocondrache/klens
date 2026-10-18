import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";
import { RouterProvider } from "@tanstack/react-router";
import { LucideProvider } from "lucide-react";

import { TooltipProvider } from "@/components/ui/tooltip";
import { queryClient } from "@/lib/query-client";
import { router } from "@/router";
import "./styles/global.css";
import "./styles/highlighting.css";

const root = document.getElementById("root");
if (!root) throw new Error("index.html has no #root element");

createRoot(root).render(
  <StrictMode>
    <ThemeProvider attribute="class" defaultTheme="dark" enableSystem disableTransitionOnChange>
      <QueryClientProvider client={queryClient}>
        <TooltipProvider delay={250}>
          {/* Icons render from 12px to 16px, and a stroke that scales with them looks faint at 12px. */}
          <LucideProvider strokeWidth={1.25} nonScalingStroke>
            <RouterProvider router={router} />
          </LucideProvider>
        </TooltipProvider>
      </QueryClientProvider>
    </ThemeProvider>
  </StrictMode>,
);
