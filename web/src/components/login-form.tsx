import { CircleAlertIcon, KeyRoundIcon } from "lucide-react";
import { useSearch } from "@tanstack/react-router";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { LogoMark } from "@/components/logo";
import { apiPath } from "@/lib/api/client";
import { cn } from "@/lib/utils";

export function LoginForm({ className, ...props }: React.ComponentProps<"div">) {
  const { error } = useSearch({ from: "/login" });
  const forbidden = error === "forbidden";

  return (
    <div className={cn("flex flex-col items-center gap-6 text-center", className)} {...props}>
      <LogoMark className="mb-2 size-12" />
      <div className="space-y-1.5">
        <h1 className="text-xl font-semibold tracking-[-0.015em]">Sign in to klens</h1>
        <p className="text-sm text-balance text-muted-foreground">
          Inspect topics, records, consumer groups, brokers, and schemas.
        </p>
      </div>
      {error ? (
        <Alert variant="destructive" className="text-left">
          <CircleAlertIcon />
          <AlertTitle>{forbidden ? "Access denied" : "Sign-in failed"}</AlertTitle>
          <AlertDescription>
            {forbidden
              ? "Your account is not assigned a klens role."
              : "Try again, or check the identity provider."}
          </AlertDescription>
        </Alert>
      ) : null}
      <Button size="lg" className="w-full" render={<a href={apiPath("/auth/login")} />}>
        <KeyRoundIcon data-icon="inline-start" />
        Continue with SSO
      </Button>
    </div>
  );
}
