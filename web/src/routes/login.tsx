import { createFileRoute } from "@tanstack/react-router";

import { LoginCover } from "@/components/login-cover";
import { LoginForm } from "@/components/login-form";
import { ModeToggle } from "@/components/mode-toggle";
import { RELEASE_URL, REPO_URL, VERSION } from "@/lib/build";

export const Route = createFileRoute("/login")({
  component: LoginPage,
});

function LoginPage() {
  return (
    <div className="relative flex min-h-svh flex-col bg-background">
      <LoginCover />

      <header className="relative flex justify-end p-4 md:p-6">
        <ModeToggle />
      </header>

      <main className="relative flex flex-1 flex-col items-center justify-center px-4 pb-24">
        <LoginForm className="w-full max-w-xs" />
      </main>

      <footer className="relative flex items-center justify-center gap-2 p-6 text-xs text-muted-foreground">
        <a
          href={RELEASE_URL}
          target="_blank"
          rel="noreferrer"
          className="numeric hover:text-foreground"
        >
          v{VERSION}
        </a>
        <span aria-hidden className="text-muted-foreground/40">
          ·
        </span>
        <a href={REPO_URL} target="_blank" rel="noreferrer" className="hover:text-foreground">
          GitHub
        </a>
      </footer>
    </div>
  );
}
