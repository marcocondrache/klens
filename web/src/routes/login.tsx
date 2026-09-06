import { LoginForm } from "@/components/login-form"
import { ModeToggle } from "@/components/mode-toggle"

export function LoginPage() {
  return (
    <div className="relative flex min-h-svh flex-col items-center justify-center gap-6 p-6 md:p-10">
      <div className="absolute top-4 right-4">
        <ModeToggle />
      </div>
      <div className="flex w-full max-w-sm flex-col gap-6">
        <div className="flex items-center justify-center">
          <span className="text-2xl font-semibold tracking-tight">klens</span>
        </div>
        <LoginForm />
      </div>
    </div>
  )
}
