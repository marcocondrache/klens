import { CircleAlertIcon } from "lucide-react";
import { useSearch } from "@tanstack/react-router";

import { PageHeader } from "@/components/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export function LoginForm({ className, ...props }: React.ComponentProps<"div">) {
  const { error } = useSearch({ from: "/login" });

  return (
    <div className={cn("flex flex-col gap-6", className)} {...props}>
      <PageHeader
        title="Sign in"
        description="Continue with your identity provider to use klens."
      />
      {error ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertTitle>Sign-in failed</AlertTitle>
          <AlertDescription>Try again, or check the identity provider.</AlertDescription>
        </Alert>
      ) : null}
      <Button className="w-full" render={<a href="/auth/login" />}>
        Continue with SSO
      </Button>
    </div>
  );
}
