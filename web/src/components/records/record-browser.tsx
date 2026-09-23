import { useState } from "react";

import { LiveRecords } from "@/components/records/live-records";
import { PagedRecords } from "@/components/records/paged-records";
import type { RecordMode } from "@/components/records/record-mode";
import { EMPTY_FILTER, type RecordFilter } from "@/components/records/record-view";
import type { TopicDetail } from "@/lib/api/types";

export function RecordBrowser({ cluster, topic }: { cluster: string; topic: TopicDetail }) {
  const [mode, setMode] = useState<RecordMode>("NEWEST");
  const [filter, setFilter] = useState<RecordFilter>(EMPTY_FILTER);

  const props = { cluster, topic, filter, onFilterChange: setFilter, onModeChange: setMode };

  return mode === "LIVE" ? <LiveRecords {...props} /> : <PagedRecords {...props} order={mode} />;
}
