import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { BrokerPage } from "@/features/brokers/broker-page";
import { brokerDetailSearch } from "@/features/brokers/search";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/nodes_/$id")({
  validateSearch: brokerDetailSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(brokerDetailSearch))] },
  component: BrokerPage,
});
