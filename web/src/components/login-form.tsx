import { useSearchParams } from "react-router"

import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Field, FieldError, FieldGroup } from "@/components/ui/field"
import { cn } from "@/lib/utils"

export function LoginForm({ className, ...props }: React.ComponentProps<"div">) {
  const [params] = useSearchParams()
  const error = params.get("error")

  return (
    <div className={cn("flex flex-col gap-6", className)} {...props}>
      <Card>
        <CardHeader>
          <CardTitle>Sign in</CardTitle>
          <CardDescription>Continue with your identity provider to use klens.</CardDescription>
        </CardHeader>
        <CardContent>
          <FieldGroup>
            {error ? (
              <Field data-invalid>
                <FieldError>Sign-in failed. Try again, or check the identity provider.</FieldError>
              </Field>
            ) : null}
            <Field>
              <Button className="w-full" render={<a href="/auth/login" />}>
                Continue with SSO
              </Button>
            </Field>
          </FieldGroup>
        </CardContent>
      </Card>
    </div>
  )
}
