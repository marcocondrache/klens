import { CircleAlertIcon } from "lucide-react";
import { useSearchParams } from "react-router";

import { PageHeader } from "@/components/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export function LoginForm({ className, ...props }: React.ComponentProps<"div">) {
  const [params] = useSearchParams();
  const error = params.get("error");

  return (
    <div className={cn("flex flex-col gap-6", className)} {...props}>
      <PageHeader
        title="Sign in"
        description="Continue with your identity provider to access your clusters."
      />
      {error ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertTitle>Sign-in failed</AlertTitle>
          <AlertDescription>Try again, or check the identity provider.</AlertDescription>
        </Alert>
      ) : null}
      <Button
        className="w-full shadow-[0_0_0_0_var(--brand)] transition-shadow duration-300 hover:shadow-[0_0_28px_-2px_var(--brand)]"
        render={<a href="/auth/login" />}
      >
        Continue with SSO
      </Button>
    </div>
  );
}
