import { useState } from "react";
import { RadioIcon } from "lucide-react";

import { Toggle } from "@/components/ui/toggle";
import { LiveRecords } from "@/components/records/live-records";
import { PagedRecords } from "@/components/records/paged-records";
import { EMPTY_FILTER, type RecordFilter } from "@/components/records/record-view";
import type { TopicDetail } from "@/lib/api/types";

export function RecordBrowser({ cluster, topic }: { cluster: string; topic: TopicDetail }) {
  const [live, setLive] = useState(false);
  const [filter, setFilter] = useState<RecordFilter>(EMPTY_FILTER);

  const Source = live ? LiveRecords : PagedRecords;

  return (
    <Source
      cluster={cluster}
      topic={topic}
      filter={filter}
      onFilterChange={setFilter}
      actions={
        <Toggle variant="outline" pressed={live} onPressedChange={setLive} aria-label="Live tail">
          <RadioIcon />
          Live
        </Toggle>
      }
    />
  );
}
