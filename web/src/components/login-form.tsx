import { CircleAlertIcon } from "lucide-react";
import { useSearch } from "@tanstack/react-router";

import { PageHeader } from "@/components/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { ssoLoginHref } from "@/lib/auth";
import { cn } from "@/lib/utils";

export function LoginForm({ className, ...props }: React.ComponentProps<"div">) {
  const { error, next } = useSearch({ from: "/login" });
  const forbidden = error === "forbidden";

  return (
    <div className={cn("flex flex-col gap-6", className)} {...props}>
      <PageHeader
        title="Sign in"
        description="Continue with your identity provider to use klens."
      />
      {error ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertTitle>{forbidden ? "Access denied" : "Sign-in failed"}</AlertTitle>
          <AlertDescription>
            {forbidden
              ? "Your account is not assigned a klens role."
              : "Try again, or check the identity provider."}
          </AlertDescription>
        </Alert>
      ) : null}
      <Button className="w-full" render={<a href={ssoLoginHref(next)} />}>
        Continue with SSO
      </Button>
    </div>
  );
}
