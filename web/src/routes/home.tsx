import { ServerOffIcon } from "lucide-react";
import { Navigate } from "@/lib/navigation";

import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { PageLoading } from "@/components/page-loading";
import { useClusters } from "@/lib/api/catalog";

export function HomePage() {
  const { data: clusters, isPending, isError } = useClusters();
  const name = clusters?.[0]?.name;

  if (isPending) {
    return (
      <PageLoading
        title="Loading clusters"
        description="Fetching broker metadata. First contact with Kafka can take a few seconds."
        slowDescription="Still waiting on brokers. Large or unreachable clusters take longer on first load."
      />
    );
  }

  if (name) {
    return <Navigate to={`/cluster/${name}/topics`} replace />;
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
