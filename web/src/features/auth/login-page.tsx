import { BuildLinks } from "@/components/build-links";
import { ModeToggle } from "@/components/mode-toggle";

import { LoginCover } from "./login-cover";
import { LoginForm } from "./login-form";

export function LoginPage() {
  return (
    <div className="relative flex min-h-svh flex-col bg-background">
      <LoginCover />

      <header className="relative flex justify-end p-4 md:p-6">
        <ModeToggle />
      </header>

      <main className="relative flex flex-1 flex-col items-center justify-center px-4 pb-24">
        <LoginForm className="w-full max-w-xs" />
      </main>

      <footer className="relative p-6">
        <BuildLinks />
      </footer>
    </div>
  );
}
