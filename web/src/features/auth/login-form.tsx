import { CircleAlertIcon, KeyRoundIcon } from "lucide-react";
import { useSearch } from "@tanstack/react-router";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { buttonVariants } from "@/components/ui/button";
import { LogoMark } from "@/components/logo";
import { SIGN_IN_PATH } from "@/lib/api/client";
import { cn } from "@/lib/utils";

import type { LoginSearch } from "./search";

export function LoginForm({ className, ...props }: React.ComponentProps<"div">) {
  const { error, from } = useSearch({ from: "/login" });
  const alert = loginAlert(error, from);

  return (
    <div className={cn("flex flex-col items-center gap-6 text-center", className)} {...props}>
      <LogoMark className="mb-2 size-12" />
      <div className="space-y-1.5">
        <h1 className="text-xl font-semibold tracking-[-0.015em]">Sign in to klens</h1>
        <p className="text-sm text-balance text-muted-foreground">
          Inspect topics, records, consumer groups, brokers, and schemas.
        </p>
      </div>
      {alert ? (
        <Alert variant="destructive" className="text-left">
          <CircleAlertIcon />
          <AlertTitle>{alert.title}</AlertTitle>
          <AlertDescription>{alert.description}</AlertDescription>
        </Alert>
      ) : null}
      <a href={SIGN_IN_PATH} className={cn(buttonVariants({ size: "lg" }), "w-full")}>
        <KeyRoundIcon data-icon="inline-start" />
        Continue with SSO
      </a>
    </div>
  );
}

function loginAlert(
  error: LoginSearch["error"],
  from: LoginSearch["from"],
): { title: string; description: string } | null {
  if (error === "forbidden") {
    return { title: "Access denied", description: "Your account has no klens role." };
  }
  if (error) {
    return { title: "Sign-in failed", description: "Try again, or check the identity provider." };
  }
  if (from === "callback") {
    return {
      title: "Session not kept",
      description:
        "The identity provider signed you in, but klens did not keep the session. Check that the session cookie reaches klens.",
    };
  }
  return null;
}
