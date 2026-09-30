import { useMemo } from "react";
import { BrushCleaningIcon, PauseIcon, PlayIcon, RadioIcon, TriangleAlertIcon } from "lucide-react";

import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { IconButton } from "@/components/icon-button";
import { RecordModeSwitch, type RecordMode } from "@/components/records/record-mode";
import {
  RecordView,
  filterPartitions,
  type RecordFilter,
  type RecordSource,
} from "@/components/records/record-view";
import { Pill } from "@/components/status";
import { useDebouncedValue } from "@/hooks/use-debounced-value";
import { apiErrorMessage } from "@/lib/api/client";
import { TAIL_BUFFER, useTail, type TailFilter } from "@/lib/api/tail";
import type { TopicDetail } from "@/lib/api/types";
import { formatNumber } from "@/lib/format";

function SkippedBadge({ skipped }: { skipped: number }) {
  return (
    <Tooltip>
      <TooltipTrigger render={<Pill tone="warn" className="cursor-default" />}>
        {formatNumber(skipped)} skipped
      </TooltipTrigger>
      <TooltipContent className="block max-w-80 py-2 leading-relaxed">
        This topic produces faster than the tail can show. Each update keeps the newest records and
        skips the rest.
      </TooltipContent>
    </Tooltip>
  );
}

type LiveRecordsProps = {
  cluster: string;
  topic: TopicDetail;
  filter: RecordFilter;
  onFilterChange: (filter: RecordFilter) => void;
  onModeChange: (mode: RecordMode) => void;
};

export function LiveRecords({
  cluster,
  topic,
  filter,
  onFilterChange,
  onModeChange,
}: LiveRecordsProps) {
  const needle = useDebouncedValue(filter.term.trim());
  const partitions = useMemo(() => filterPartitions(topic, filter), [topic, filter]);

  const tailFilter = useMemo<TailFilter>(
    () => ({
      topic: topic.name,
      partitions,
      contains: needle || null,
      schemaId: filter.schemaId,
    }),
    [topic.name, partitions, filter.schemaId, needle],
  );
  const tail = useTail(cluster, tailFilter);

  const empty =
    partitions?.length === 0
      ? {
          title: "No partitions to follow",
          description: "The partition filter excludes every partition.",
        }
      : tail.paused
        ? {
            title: "Live tail paused",
            description: "Resume to follow new records. Records produced while paused are missed.",
          }
        : tail.status === "error"
          ? { title: "Live tail stopped", description: "Retry to reconnect." }
          : {
              title: "Waiting for records",
              description: `New records appear here as they arrive, newest first. Only the last ${formatNumber(TAIL_BUFFER)} are kept.`,
            };

  const source: RecordSource = {
    records: tail.records,
    scope: JSON.stringify(tailFilter),
    obfuscated: tail.obfuscated,
    loading: false,
    refreshing: tail.status === "connecting" || tail.status === "reconnecting",
  };

  return (
    <RecordView
      cluster={cluster}
      topic={topic}
      source={source}
      filter={filter}
      onFilterChange={onFilterChange}
      notice={
        tail.status === "error" ? (
          <Alert variant="destructive">
            <TriangleAlertIcon />
            <AlertTitle>Live tail stopped</AlertTitle>
            <AlertDescription>
              {apiErrorMessage(tail.error, "The live tail ended.")}
            </AlertDescription>
            <AlertAction>
              <Button variant="outline" size="sm" onClick={tail.retry}>
                Retry
              </Button>
            </AlertAction>
          </Alert>
        ) : null
      }
      actions={
        <>
          {tail.skipped > 0 ? <SkippedBadge skipped={tail.skipped} /> : null}
          <IconButton
            label={tail.paused ? "Resume live tail" : "Pause live tail"}
            tooltip={tail.paused ? "Resume" : "Pause"}
            size="icon"
            onClick={tail.paused ? tail.resume : tail.pause}
          >
            {tail.paused ? <PlayIcon className="size-3.5" /> : <PauseIcon className="size-3.5" />}
          </IconButton>
          <IconButton
            label="Clear records"
            tooltip="Clear"
            size="icon"
            disabled={tail.records.length === 0 && tail.skipped === 0}
            onClick={tail.clear}
          >
            <BrushCleaningIcon className="size-3.5" />
          </IconButton>
          <RecordModeSwitch value="LIVE" onChange={onModeChange} status={tail.status} />
        </>
      }
      emptyState={
        <Empty className="py-10">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <RadioIcon />
            </EmptyMedia>
            <EmptyTitle>{empty.title}</EmptyTitle>
            <EmptyDescription>{empty.description}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      }
    />
  );
}
