import { useId, useState, type FormEvent, type ReactElement, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon } from "lucide-react";

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
import { apiErrorMessage, clusterPathname, del } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { TopicDetail } from "@/lib/api/types";

const ALL = "all";
const OFFSET = /^\d+$/;

export type RecordCut = { partition: number | null; before: number | null };

export function DeleteRecordsDialog({
  cluster,
  topic,
  cut = { partition: null, before: null },
  trigger,
  children,
}: {
  cluster: string;
  topic: TopicDetail;
  cut?: RecordCut;
  trigger: ReactElement;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={trigger}>{children}</DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DeleteRecordsForm
          cluster={cluster}
          topic={topic}
          cut={cut}
          onDeleted={() => setOpen(false)}
        />
      </DialogContent>
    </Dialog>
  );
}

function DeleteRecordsForm({
  cluster,
  topic,
  cut,
  onDeleted,
}: {
  cluster: string;
  topic: TopicDetail;
  cut: RecordCut;
  onDeleted: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [partition, setPartition] = useState(cut.partition === null ? ALL : String(cut.partition));
  const [before, setBefore] = useState(cut.before === null ? "" : String(cut.before));
  const [typed, setTyped] = useState("");
  const compacted = topic.cleanupPolicy === "COMPACT";

  const remove = useMutation({
    mutationFn: () =>
      del(clusterPathname(cluster, "topics", encodeURIComponent(topic.name), "records"), {
        partition: partition === ALL ? undefined : Number(partition),
        before: before === "" ? undefined : Number(before),
      }),
    onSuccess: () =>
      Promise.all([
        queryClient.invalidateQueries({ queryKey: keys.topic(cluster, topic.name), exact: true }),
        queryClient.invalidateQueries({ queryKey: keys.topicRecords(cluster, topic.name) }),
      ]),
  });

  const partitions = [
    { value: ALL, label: "All partitions" },
    ...topic.partitions.map((entry) => ({ value: String(entry.id), label: String(entry.id) })),
  ];
  const badBefore = before !== "" && !OFFSET.test(before);
  const ready = !compacted && !badBefore && typed === topic.name && !remove.isPending;
  const scope = partition === ALL ? "every partition" : `partition ${partition}`;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    remove.mutate(undefined, { onSuccess: onDeleted });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Delete records</DialogTitle>
        <DialogDescription>
          {before === "" || badBefore
            ? `This deletes every record in ${scope}`
            : `This deletes the records before offset ${before} in ${scope}`}{" "}
          of <span className="font-mono">{topic.name}</span>, and cannot be undone.
        </DialogDescription>
      </DialogHeader>

      {compacted ? (
        <Alert>
          <CircleAlertIcon />
          <AlertDescription>
            Kafka keeps the records of a compacted topic. Add delete to its cleanup.policy to delete
            records.
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="grid grid-cols-2 gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-partition`}>Partition</Label>
          <Select
            items={partitions}
            value={partition}
            onValueChange={(next) => {
              if (next !== null) setPartition(next);
            }}
          >
            <SelectTrigger id={`${id}-partition`} className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {partitions.map((entry) => (
                <SelectItem key={entry.value} value={entry.value}>
                  {entry.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-before`}>Before offset</Label>
          <Input
            id={`${id}-before`}
            inputMode="numeric"
            autoComplete="off"
            placeholder="End of log"
            className="numeric"
            value={before}
            aria-invalid={badBefore || undefined}
            onChange={(event) => setBefore(event.target.value.trim())}
          />
        </div>
      </div>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-name`}>
          Type <span className="font-mono">{topic.name}</span> to confirm
        </Label>
        <Input
          id={`${id}-name`}
          autoFocus
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          value={typed}
          onChange={(event) => setTyped(event.target.value)}
        />
      </div>

      {remove.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription>
            {apiErrorMessage(remove.error, "Failed to delete the records.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" variant="destructive" disabled={!ready}>
          {remove.isPending ? <Spinner data-icon="inline-start" /> : null}
          Delete records
        </Button>
      </DialogFooter>
    </form>
  );
}
