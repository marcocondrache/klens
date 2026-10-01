import { createFileRoute, stripSearchParams } from "@tanstack/react-router";

import { topicsSearch } from "@/features/topics/search";
import { TopicsPage } from "@/features/topics/topics-page";
import { searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/topics")({
  validateSearch: topicsSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(topicsSearch))] },
  component: TopicsPage,
});
