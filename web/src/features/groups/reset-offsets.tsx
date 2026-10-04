import { useId, useState, type FormEvent } from "react";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, RotateCcwIcon } from "lucide-react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
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

export function ResetOffsetsDialog({ cluster, group }: { cluster: string; group: GroupDetail }) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button variant="outline" />}>
        <RotateCcwIcon data-icon="inline-start" />
        Reset offsets
      </DialogTrigger>
      <DialogContent className="sm:max-w-xl">
        <ResetOffsetsForm cluster={cluster} group={group} onReset={() => setOpen(false)} />
      </DialogContent>
    </Dialog>
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
  const ready = request !== null && !active && current && moves.length > 0 && !reset.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready || request === null) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    reset.mutate({ ...request, dryRun: false }, { onSuccess: onReset });
  }

  return (
    <form className="grid min-w-0 gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Reset offsets</DialogTitle>
        <DialogDescription>
          This moves the committed offsets of <span className="font-mono">{group.id}</span>. Its
          consumers continue from the new offsets when they rejoin.
        </DialogDescription>
      </DialogHeader>

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
        <div className="grid min-w-0 gap-1.5">
          <Label htmlFor={`${id}-topic`}>Topic</Label>
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
        </div>
        <div className="grid min-w-0 gap-1.5">
          <Label htmlFor={`${id}-partition`}>Partition</Label>
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
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="grid min-w-0 gap-1.5">
          <Label htmlFor={`${id}-target`}>Reset to</Label>
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
        </div>
        <TargetInput id={`${id}-value`} kind={kind} value={value} onChange={setValue} />
      </div>

      {moves.length > 0 ? (
        <PlanTable moves={moves} stale={!current} />
      ) : current ? (
        <p className="text-sm text-muted-foreground">The group has no committed offsets to move.</p>
      ) : null}

      {preview.isError || reset.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription>
            {reset.isError
              ? apiErrorMessage(reset.error, "Failed to reset the offsets.")
              : apiErrorMessage(preview.error, "Failed to plan the reset.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={!ready}>
          {reset.isPending ? <Spinner data-icon="inline-start" /> : null}
          Reset offsets
        </Button>
      </DialogFooter>
    </form>
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

  return (
    <div className="grid min-w-0 gap-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        autoFocus
        type={timestamp ? "datetime-local" : "text"}
        inputMode={timestamp ? undefined : "numeric"}
        autoComplete="off"
        className={timestamp ? undefined : "numeric"}
        value={value}
        aria-invalid={(value !== "" && resetTarget(kind, value) === null) || undefined}
        onChange={(event) => onChange(timestamp ? event.target.value : event.target.value.trim())}
      />
    </div>
  );
}

function PlanTable({ moves, stale }: { moves: OffsetMove[]; stale: boolean }) {
  return (
    <div
      className="max-h-64 overflow-auto rounded-md border data-[stale=true]:opacity-60"
      data-stale={stale}
    >
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Topic</TableHead>
            <TableHead className="text-right">Partition</TableHead>
            <TableHead className="text-right">Committed</TableHead>
            <TableHead className="text-right">New</TableHead>
            <TableHead className="text-right">Lag after</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {moves.map((move) => (
            <TableRow key={`${move.topic}-${move.partition}`}>
              <TableCell className="max-w-48 truncate font-mono">{move.topic}</TableCell>
              <TableCell className="numeric text-right">{move.partition}</TableCell>
              <TableCell className="numeric text-right text-muted-foreground">
                {move.currentOffset === null ? "none" : formatNumber(move.currentOffset)}
              </TableCell>
              <TableCell className="numeric text-right">{formatNumber(move.newOffset)}</TableCell>
              <TableCell className="numeric text-right text-muted-foreground">
                {formatNumber(move.endOffset - move.newOffset)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
