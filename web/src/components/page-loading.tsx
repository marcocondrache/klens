import { useEffect, useState } from "react";

import { LogoMark } from "@/components/logo";

export function PageLoading({
  title,
  description,
  slowDescription,
}: {
  title: string;
  description: string;
  slowDescription?: string;
}) {
  const [slow, setSlow] = useState(false);

  useEffect(() => {
    if (!slowDescription) {
      return;
    }

    const id = window.setTimeout(() => setSlow(true), 4000);
    return () => window.clearTimeout(id);
  }, [slowDescription]);

  return (
    <div
      className="flex min-h-svh flex-col items-center justify-center gap-4 px-6"
      role="status"
      aria-live="polite"
    >
      <LogoMark className="size-8 motion-safe:animate-pulse" />
      <div className="max-w-sm space-y-1 text-center">
        <p className="text-sm font-medium">{title}</p>
        <p className="text-sm text-balance text-muted-foreground">
          {slow && slowDescription ? slowDescription : description}
        </p>
      </div>
    </div>
  );
}

export function ClustersLoading() {
  return (
    <PageLoading
      title="Loading clusters"
      description="Loading the cluster list."
      slowDescription="The API is not responding."
    />
  );
}

export function CatalogLoading() {
  return (
    <PageLoading
      title="Loading catalog"
      description="Waiting for the first cluster snapshot."
      slowDescription="The cluster is still not ready."
    />
  );
}
