import { ActivityIcon, HeartPulseIcon, RecycleIcon } from "lucide-react";

import { type FilterField } from "@/components/data-table/filters";
import { StatusDot } from "@/components/status";
import type { TopicRow } from "@/lib/api/types";
import { formatCleanupPolicy } from "@/lib/format";

import type { TopicFilter } from "./search";

export const TOPIC_FILTERS: Array<FilterField<TopicRow, TopicFilter>> = [
  {
    id: "policy",
    label: "Policy",
    plural: "policies",
    icon: RecycleIcon,
    options: [
      { value: "delete", label: "delete" },
      { value: "compact", label: "compact" },
    ],
    accessor: (topic) => formatCleanupPolicy(topic.cleanupPolicy).split(","),
  },
  {
    id: "health",
    label: "Health",
    plural: "states",
    icon: HeartPulseIcon,
    options: [
      { value: "under-replicated", label: "Under-replicated", icon: <StatusDot tone="warn" /> },
      { value: "in-sync", label: "In sync", icon: <StatusDot tone="ok" /> },
    ],
    accessor: (topic) => (topic.underReplicated ? "under-replicated" : "in-sync"),
  },
  {
    id: "activity",
    label: "Activity",
    plural: "states",
    icon: ActivityIcon,
    options: [
      { value: "active", label: "Producing", icon: <StatusDot tone="ok" /> },
      { value: "idle", label: "Idle", icon: <StatusDot tone="idle" /> },
    ],
    accessor: (topic) => (topic.rate === 0 ? "idle" : "active"),
  },
];

export function topicMatches(topic: TopicRow, needle: string) {
  return topic.name.toLowerCase().includes(needle);
}
