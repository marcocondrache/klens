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
import { RecordView, type RecordFilter, type RecordSource } from "@/components/records/record-view";
import { Pill, StatusDot } from "@/components/status";
import { apiErrorMessage } from "@/lib/api/client";
import { TAIL_BUFFER, useTail, type TailFilter, type TailStatus } from "@/lib/api/tail";
import type { TopicDetail } from "@/lib/api/types";
import { formatNumber } from "@/lib/format";
import type { Tone } from "@/lib/tone";

const STATUS: Record<TailStatus, { label: string; tone: Tone }> = {
  idle: { label: "Paused", tone: "idle" },
  connecting: { label: "Connecting…", tone: "warn" },
  live: { label: "Live", tone: "ok" },
  reconnecting: { label: "Reconnecting…", tone: "warn" },
  error: { label: "Stopped", tone: "error" },
};

function TailStatusPill({ status }: { status: TailStatus }) {
  const { label, tone } = STATUS[status];

  return (
    <Pill tone={tone}>
      <StatusDot tone={tone} pulse={status === "live"} />
      {label}
    </Pill>
  );
}

function SkippedBadge({ skipped }: { skipped: number }) {
  return (
    <Tooltip>
      <TooltipTrigger render={<Pill tone="warn" className="cursor-default" />}>
        {formatNumber(skipped)} skipped
      </TooltipTrigger>
      <TooltipContent className="block max-w-80 py-2 leading-relaxed">
        This topic produces faster than a live tail shows. The tail samples it: each update keeps
        the newest records and passes over the rest.
      </TooltipContent>
    </Tooltip>
  );
}

type LiveRecordsProps = {
  cluster: string;
  topic: TopicDetail;
  filter: RecordFilter;
  onFilterChange: (filter: RecordFilter) => void;
  actions?: React.ReactNode;
};

export function LiveRecords({ cluster, topic, filter, onFilterChange, actions }: LiveRecordsProps) {
  const tailFilter = useMemo<TailFilter>(
    () => ({
      topic: topic.name,
      partition: filter.partition,
      contains: filter.term.trim() || null,
      schemaId: filter.schemaId,
    }),
    [topic.name, filter],
  );
  const tail = useTail(cluster, tailFilter);

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
          <TailStatusPill status={tail.status} />
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="outline"
                  size="icon"
                  aria-label={tail.paused ? "Resume live tail" : "Pause live tail"}
                  onClick={tail.paused ? tail.resume : tail.pause}
                />
              }
            >
              {tail.paused ? <PlayIcon /> : <PauseIcon />}
            </TooltipTrigger>
            <TooltipContent>{tail.paused ? "Resume" : "Pause"}</TooltipContent>
          </Tooltip>
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="outline"
                  size="icon"
                  aria-label="Clear records"
                  disabled={tail.records.length === 0 && tail.skipped === 0}
                  onClick={tail.clear}
                />
              }
            >
              <BrushCleaningIcon />
            </TooltipTrigger>
            <TooltipContent>Clear</TooltipContent>
          </Tooltip>
          {actions}
        </>
      }
      emptyState={
        <Empty className="py-10">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <RadioIcon />
            </EmptyMedia>
            <EmptyTitle>
              {tail.paused
                ? "Live tail paused"
                : tail.status === "error"
                  ? "Live tail stopped"
                  : "Waiting for records"}
            </EmptyTitle>
            <EmptyDescription>
              {tail.paused
                ? "Resume to follow the topic from its current end."
                : tail.status === "error"
                  ? "Retry to follow the topic again."
                  : `Following ${topic.name} from its current end. New records show here as they arrive, newest first. The last ${formatNumber(TAIL_BUFFER)} stay on screen.`}
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      }
    />
  );
}
