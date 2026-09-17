import { ServerOffIcon } from "lucide-react";
import { Navigate, createFileRoute } from "@tanstack/react-router";

import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { CatalogLoading, ClustersLoading } from "@/components/page-loading";
import { useClusters } from "@/lib/api/catalog";
import { isFirstCatalogPending } from "@/lib/clusters";

export const Route = createFileRoute("/")({
  component: HomePage,
});

function HomePage() {
  const { data: clusters, isPending, isError } = useClusters();
  const first = clusters?.[0];

  if (isPending) {
    return <ClustersLoading />;
  }

  if (isFirstCatalogPending(first)) {
    return <CatalogLoading />;
  }

  if (first) {
    return <Navigate to="/cluster/$cluster/topics" params={{ cluster: first.cluster }} replace />;
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
