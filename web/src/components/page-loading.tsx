import { useEffect, useState } from "react";

export function PageLoading({ label, slowLabel }: { label: string; slowLabel?: string }) {
  const [slow, setSlow] = useState(false);

  useEffect(() => {
    if (!slowLabel) {
      return;
    }

    const id = window.setTimeout(() => setSlow(true), 4000);
    return () => window.clearTimeout(id);
  }, [slowLabel]);

  return (
    <div
      className="flex min-h-svh flex-col items-center justify-center gap-4 px-6"
      role="status"
      aria-live="polite"
    >
      <svg
        viewBox="0 0 64 64"
        fill="none"
        aria-hidden
        className="size-7 motion-safe:animate-spin motion-safe:[animation-duration:0.9s] motion-reduce:animate-pulse"
      >
        <circle cx="32" cy="32" r="24" stroke="var(--brand)" strokeOpacity="0.2" strokeWidth="8" />
        <circle
          cx="32"
          cy="32"
          r="24"
          stroke="var(--brand)"
          strokeWidth="8"
          strokeLinecap="round"
          strokeDasharray="38 151"
        />
      </svg>
      <p key={slow ? "slow" : "label"} className="text-sm text-muted-foreground animate-in fade-in">
        {slow && slowLabel ? slowLabel : label}
      </p>
    </div>
  );
}

export function ClustersLoading() {
  return (
    <PageLoading label="Loading clusters…" slowLabel="Still waiting for the API to respond…" />
  );
}

export function CatalogLoading() {
  return (
    <PageLoading
      label="Connecting to the cluster…"
      slowLabel="The cluster is taking longer than usual…"
    />
  );
}
