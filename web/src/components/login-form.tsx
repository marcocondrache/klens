import { CircleAlertIcon } from "lucide-react";
import { useSearch } from "@tanstack/react-router";

import { PageHeader } from "@/components/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { SIGN_IN_PATH } from "@/lib/api/client";
import { cn } from "@/lib/utils";

export function LoginForm({ className, ...props }: React.ComponentProps<"div">) {
  const { error, from } = useSearch({ from: "/login" });
  const alert = loginAlert(error, from);

  return (
    <div className={cn("flex flex-col gap-6", className)} {...props}>
      <PageHeader title="Sign in" description="Use your identity provider." />
      {alert ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertTitle>{alert.title}</AlertTitle>
          <AlertDescription>{alert.description}</AlertDescription>
        </Alert>
      ) : null}
      <Button className="w-full" render={<a href={SIGN_IN_PATH} />}>
        Continue with SSO
      </Button>
    </div>
  );
}

function loginAlert(
  error: string | undefined,
  from: "callback" | undefined,
): { title: string; description: string } | null {
  if (error === "forbidden") {
    return { title: "Access denied", description: "Your account is not assigned a klens role." };
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
