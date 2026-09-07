import { useEffect, useState } from "react"

import { Spinner } from "@/components/ui/spinner"

export function PageLoading({
  title,
  description,
  slowDescription,
}: {
  title: string
  description: string
  slowDescription?: string
}) {
  const [slow, setSlow] = useState(false)

  useEffect(() => {
    if (!slowDescription) {
      return
    }

    const id = window.setTimeout(() => setSlow(true), 4000)
    return () => window.clearTimeout(id)
  }, [slowDescription])

  return (
    <div className="flex min-h-svh flex-col items-center justify-center gap-4 px-6" role="status" aria-live="polite">
      <Spinner className="size-6" aria-hidden />
      <div className="max-w-sm space-y-1 text-center">
        <p className="text-sm font-medium">{title}</p>
        <p className="text-sm text-balance text-muted-foreground">
          {slow && slowDescription ? slowDescription : description}
        </p>
      </div>
    </div>
  )
}
