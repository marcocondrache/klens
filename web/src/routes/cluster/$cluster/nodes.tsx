import { createFileRoute } from "@tanstack/react-router";

import { BrokersPage } from "@/features/brokers/brokers-page";

export const Route = createFileRoute("/cluster/$cluster/nodes")({
  component: BrokersPage,
});
