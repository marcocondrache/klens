import { ServerOffIcon } from "lucide-react"
import { Navigate } from "react-router"

import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty"
import { Spinner } from "@/components/ui/spinner"
import { useClusters } from "@/lib/api/queries"

export function HomePage() {
  const { data: clusters, isPending, isError } = useClusters()
  const name = clusters?.[0]?.name

  if (isPending) {
    return (
      <div className="flex min-h-svh items-center justify-center">
        <Spinner className="size-6" />
      </div>
    )
  }

  if (name) {
    return <Navigate to={`/cluster/${name}/topics`} replace />
  }

  return (
    <Empty className="min-h-svh">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <ServerOffIcon />
        </EmptyMedia>
        <EmptyTitle>{isError ? "Could not load clusters" : "No clusters configured"}</EmptyTitle>
        <EmptyDescription>
          {isError
            ? "The GraphQL API did not respond. Start the klens service and try again."
            : "Add a cluster to config.yaml and restart the service."}
        </EmptyDescription>
      </EmptyHeader>
    </Empty>
  )
}
