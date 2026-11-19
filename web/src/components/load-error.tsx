import type { ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, RefreshCwIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";

export function LoadError({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();

  // Only the failed queries refetch, so a retry does not reload every healthy view.
  const retry = () =>
    queryClient.refetchQueries({
      type: "active",
      predicate: (query) => query.state.status === "error",
    });

  return (
    <Empty className="py-10" role="alert">
      <EmptyHeader>
        <EmptyMedia variant="icon" className="text-destructive">
          <CircleAlertIcon />
        </EmptyMedia>
        <EmptyTitle>Unable to load</EmptyTitle>
        <EmptyDescription>{children}</EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        <Button variant="outline" size="sm" onClick={() => void retry()}>
          <RefreshCwIcon data-icon="inline-start" />
          Try again
        </Button>
      </EmptyContent>
    </Empty>
  );
}
