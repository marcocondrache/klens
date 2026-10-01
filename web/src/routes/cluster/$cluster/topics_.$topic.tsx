import { createFileRoute } from "@tanstack/react-router";

import { topicDetailSearch } from "@/features/topics/search";
import { TopicPage } from "@/features/topics/topic-page";

export const Route = createFileRoute("/cluster/$cluster/topics_/$topic")({
  validateSearch: topicDetailSearch,
  component: TopicPage,
});
