import { useState } from "react";

import type { TopicDetail } from "@/lib/api/types";

import { LiveRecords } from "./live-records";
import { PagedRecords } from "./paged-records";
import type { RecordMode } from "./record-mode";
import { EMPTY_FILTER, type RecordFilter } from "./record-view";

export function RecordBrowser({ cluster, topic }: { cluster: string; topic: TopicDetail }) {
  const [mode, setMode] = useState<RecordMode>("NEWEST");
  const [filter, setFilter] = useState<RecordFilter>(EMPTY_FILTER);

  const props = { cluster, topic, filter, onFilterChange: setFilter, onModeChange: setMode };

  return mode === "LIVE" ? <LiveRecords {...props} /> : <PagedRecords {...props} order={mode} />;
}
