import { useId, useState } from "react";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createColumnHelper } from "@tanstack/react-table";
import { CircleAlertIcon, RotateCcwIcon } from "lucide-react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldError, FieldLabel, FieldTitle } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { FieldCount } from "@/components/field-count";
import { FormSheetContent, SheetForm } from "@/components/write-form";
import { hasMembers } from "@/features/groups/group-state";
import { useDebouncedValue } from "@/hooks/use-debounced-value";
import { apiErrorMessage, clusterPathname, patchAndRead, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { GroupDetail, OffsetMove, ResetOffsets, ResetTarget } from "@/lib/api/types";
import { formatNumber } from "@/lib/format";
import { cn } from "@/lib/utils";

const ALL = "all";
const OFFSET = /^\d+$/;
const SHIFT = /^[+-]?\d+$/;

type TargetKind = ResetTarget["kind"];

const TARGETS: { value: TargetKind; label: string }[] = [
  { value: "EARLIEST", label: "Earliest offset" },
  { value: "LATEST", label: "Latest offset" },
  { value: "OFFSET", label: "Offset" },
  { value: "SHIFT", label: "Shift by" },
  { value: "TIMESTAMP", label: "Date and time" },
];

function resetTarget(kind: TargetKind, value: string): ResetTarget | null {
  switch (kind) {
    case "EARLIEST":
    case "LATEST":
      return { kind };
    case "OFFSET":
      return OFFSET.test(value) ? { kind, offset: Number(value) } : null;
    case "SHIFT":
      return SHIFT.test(value) ? { kind, by: Number(value) } : null;
    case "TIMESTAMP": {
      const timestamp = new Date(value).getTime();
      return Number.isFinite(timestamp) ? { kind, timestamp } : null;
    }
  }
}

export function ResetOffsetsSheet({ cluster, group }: { cluster: string; group: GroupDetail }) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger render={<Button variant="outline" />}>
        <RotateCcwIcon data-icon="inline-start" />
        Reset offsets
      </SheetTrigger>
      <FormSheetContent wide>
        <ResetOffsetsForm cluster={cluster} group={group} onReset={() => setOpen(false)} />
      </FormSheetContent>
    </Sheet>
  );
}

function ResetOffsetsForm({
  cluster,
  group,
  onReset,
}: {
  cluster: string;
  group: GroupDetail;
  onReset: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [topic, setTopic] = useState(ALL);
  const [partition, setPartition] = useState(ALL);
  const [kind, setKind] = useState<TargetKind>("EARLIEST");
  const [value, setValue] = useState("");
  const path = clusterPathname(cluster, "group-offsets", resourceId(group.id));
  const active = hasMembers(group.state);

  const to = resetTarget(kind, value);
  const request: ResetOffsets | null = to && {
    topic: topic === ALL ? undefined : topic,
    partitions: topic === ALL || partition === ALL ? [] : [Number(partition)],
    to,
    dryRun: true,
  };
  const planned = JSON.stringify(request);
  const debounced = useDebouncedValue(planned);

  const preview = useQuery({
    queryKey: keys.groupReset(cluster, group.id, debounced),
    queryFn: () => patchAndRead<OffsetMove[]>(path, JSON.parse(debounced)),
    enabled: request !== null && debounced === planned,
    placeholderData: keepPreviousData,
    staleTime: 0,
  });

  const reset = useMutation({
    mutationFn: (apply: ResetOffsets) => patchAndRead<OffsetMove[]>(path, apply),
    onSuccess: () =>
      Promise.all([
        queryClient.invalidateQueries({ queryKey: keys.group(cluster, group.id), exact: true }),
        queryClient.invalidateQueries({ queryKey: keys.groupRows(cluster), exact: true }),
      ]),
  });

  const topics = [...new Set(group.offsets.map((offset) => offset.topic))].sort();
  const topicItems = [
    { value: ALL, label: "Every committed topic" },
    ...topics.map((name) => ({ value: name, label: name })),
  ];
  const partitionItems = [
    { value: ALL, label: "All partitions" },
    ...group.offsets
      .filter((offset) => offset.topic === topic)
      .map((offset) => offset.partition)
      .sort((left, right) => left - right)
      .map((id) => ({ value: String(id), label: String(id) })),
  ];
  const moves = preview.data ?? [];
  const current = preview.isSuccess && !preview.isPlaceholderData && debounced === planned;

  return (
    <SheetForm
      title="Reset offsets"
      description={
        <>
          This moves the committed offsets of <span className="font-mono">{group.id}</span>. Its
          consumers continue from the new offsets when they rejoin.
        </>
      }
      error={
        reset.isError
          ? apiErrorMessage(reset.error, "Failed to reset the offsets.")
          : preview.isError
            ? apiErrorMessage(preview.error, "Failed to plan the reset.")
            : null
      }
      submit={{
        label: "Reset offsets",
        pending: reset.isPending,
        disabled: request === null || active || !current || moves.length === 0,
      }}
      onSubmit={() => {
        if (request === null) return;
        // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
        reset.mutate({ ...request, dryRun: false }, { onSuccess: onReset });
      }}
    >
      {active ? (
        <Alert>
          <CircleAlertIcon />
          <AlertDescription>
            Kafka only takes new offsets for a group without members. Stop its consumers to reset
            the offsets you preview here.
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="grid grid-cols-2 gap-3">
        <Field>
          <FieldLabel htmlFor={`${id}-topic`}>Topic</FieldLabel>
          <Select
            items={topicItems}
            value={topic}
            onValueChange={(next) => {
              if (next === null) return;
              setTopic(next);
              setPartition(ALL);
            }}
          >
            <SelectTrigger
              id={`${id}-topic`}
              className={cn("w-full", topic !== ALL && "font-mono")}
            >
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {topicItems.map((item) => (
                <SelectItem
                  key={item.value}
                  value={item.value}
                  className={cn(item.value !== ALL && "font-mono")}
                >
                  {item.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field data-disabled={topic === ALL || undefined}>
          <FieldLabel htmlFor={`${id}-partition`}>Partition</FieldLabel>
          <Select
            items={partitionItems}
            value={partition}
            disabled={topic === ALL}
            onValueChange={(next) => {
              if (next !== null) setPartition(next);
            }}
          >
            <SelectTrigger id={`${id}-partition`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {partitionItems.map((item) => (
                <SelectItem key={item.value} value={item.value}>
                  {item.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <Field>
          <FieldLabel htmlFor={`${id}-target`}>Reset to</FieldLabel>
          <Select
            items={TARGETS}
            value={kind}
            onValueChange={(next) => {
              if (next === null) return;
              setKind(next);
              setValue("");
            }}
          >
            <SelectTrigger id={`${id}-target`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {TARGETS.map((item) => (
                <SelectItem key={item.value} value={item.value}>
                  {item.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <TargetInput id={`${id}-value`} kind={kind} value={value} onChange={setValue} />
      </div>

      <Field aria-labelledby={`${id}-preview`} className="min-h-0 flex-1">
        <FieldTitle id={`${id}-preview`}>
          Preview
          <FieldCount value={moves.length} />
        </FieldTitle>
        {moves.length > 0 ? (
          <PlanTable moves={moves} stale={!current} />
        ) : current ? (
          <FieldDescription>The group has no committed offsets to move.</FieldDescription>
        ) : request === null ? (
          <FieldDescription>Enter a target to see the new offsets.</FieldDescription>
        ) : null}
      </Field>
    </SheetForm>
  );
}

function TargetInput({
  id,
  kind,
  value,
  onChange,
}: {
  id: string;
  kind: TargetKind;
  value: string;
  onChange: (value: string) => void;
}) {
  if (kind === "EARLIEST" || kind === "LATEST") return null;
  const timestamp = kind === "TIMESTAMP";
  const label = { OFFSET: "Offset", SHIFT: "Records, negative to go back", TIMESTAMP: "Time" }[
    kind
  ];
  const error = {
    OFFSET: "Enter a whole number.",
    SHIFT: "Enter a whole number such as 100 or -100.",
    TIMESTAMP: "Enter a date with a four-digit year.",
  }[kind];
  const invalid = value !== "" && resetTarget(kind, value) === null;

  return (
    <Field data-invalid={invalid || undefined}>
      <FieldLabel htmlFor={id}>{label}</FieldLabel>
      <Input
        id={id}
        data-autofocus
        type={timestamp ? "datetime-local" : "text"}
        inputMode={timestamp ? undefined : "numeric"}
        autoComplete="off"
        className={timestamp ? undefined : "numeric"}
        value={value}
        aria-invalid={invalid || undefined}
        aria-describedby={invalid ? `${id}-error` : undefined}
        onChange={(event) => onChange(timestamp ? event.target.value : event.target.value.trim())}
      />
      {invalid ? <FieldError id={`${id}-error`}>{error}</FieldError> : null}
    </Field>
  );
}

const planColumnHelper = createColumnHelper<DataTableFeatures, OffsetMove>();

const planColumns = planColumnHelper.columns([
  planColumnHelper.accessor("topic", {
    header: "Topic",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  planColumnHelper.accessor("partition", {
    header: "Partition",
    meta: { align: "right", width: "6rem" },
  }),
  planColumnHelper.accessor((move) => move.currentOffset ?? -1, {
    id: "committed",
    header: "Committed",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => (
      <span className="text-muted-foreground">
        {row.original.currentOffset === null ? "none" : formatNumber(row.original.currentOffset)}
      </span>
    ),
  }),
  planColumnHelper.accessor("newOffset", {
    header: "New",
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  planColumnHelper.accessor((move) => move.endOffset - move.newOffset, {
    id: "lag",
    header: "Lag after",
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{formatNumber(getValue())}</span>
    ),
  }),
]);

function PlanTable({ moves, stale }: { moves: OffsetMove[]; stale: boolean }) {
  return (
    <div className="flex min-h-48 flex-1 flex-col data-[stale=true]:opacity-60" data-stale={stale}>
      <DataTable
        columns={planColumns}
        data={moves}
        getRowId={(move) => `${move.topic}-${move.partition}`}
        defaultSort={{ id: "topic", direction: "asc" }}
      />
    </div>
  );
}
