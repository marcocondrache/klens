import { ServerOffIcon } from "lucide-react";
import { Navigate, createFileRoute } from "@tanstack/react-router";

import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { PageLoading } from "@/components/page-loading";
import { useClusters } from "@/lib/api/catalog";

export const Route = createFileRoute("/")({
  component: HomePage,
});

function HomePage() {
  const { data: clusters, isPending, isError } = useClusters();
  const name = clusters?.[0];

  if (isPending) {
    return (
      <PageLoading
        title="Loading clusters"
        description="Reading the configured cluster list."
        slowDescription="The GraphQL API is not responding."
      />
    );
  }

  if (name) {
    return <Navigate to="/cluster/$cluster/topics" params={{ cluster: name }} replace />;
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
  );
}
