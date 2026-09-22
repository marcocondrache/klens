import { createFileRoute } from "@tanstack/react-router";

import { LoginCover } from "@/components/login-cover";
import { LoginForm } from "@/components/login-form";
import { ModeToggle } from "@/components/mode-toggle";
import { parseLoginSearch } from "@/lib/route-search";

export const Route = createFileRoute("/login")({
  validateSearch: parseLoginSearch,
  component: LoginPage,
});

function LoginPage() {
  return (
    <div className="grid min-h-svh lg:grid-cols-2">
      <div className="flex flex-col gap-4 p-6 md:p-10">
        <div className="flex items-center justify-between">
          <a href="/" className="flex items-center gap-2 font-heading font-semibold tracking-tight">
            <img src="/favicon.svg" alt="" className="size-6" />
            klens
          </a>
          <ModeToggle />
        </div>
        <div className="flex flex-1 items-center justify-center">
          <div className="w-full max-w-sm">
            <LoginForm />
          </div>
        </div>
      </div>
      <LoginCover />
    </div>
  );
}
