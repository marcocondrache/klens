import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon } from "lucide-react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Field } from "@/components/field";
import { DialogForm, TypeToConfirm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, del } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { TopicDetail } from "@/lib/api/types";

const ALL = "all";
const OFFSET = /^\d+$/;

export type RecordCut = { partition: number | null; before: number | null };

const EVERYTHING: RecordCut = { partition: null, before: null };

export function DeleteRecordsDialog({
  cluster,
  topic,
  cut = EVERYTHING,
  open,
  onOpenChange,
}: {
  cluster: string;
  topic: TopicDetail;
  cut?: RecordCut;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DeleteRecordsForm
          cluster={cluster}
          topic={topic}
          cut={cut}
          onDeleted={() => onOpenChange(false)}
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
  const scope = partition === ALL ? "every partition" : `partition ${partition}`;

  return (
    <DialogForm
      title="Delete records"
      description={
        <>
          {before === "" || badBefore
            ? `This deletes every record in ${scope}`
            : `This deletes the records before offset ${before} in ${scope}`}{" "}
          of <span className="font-mono">{topic.name}</span>, and cannot be undone.
        </>
      }
      error={remove.isError ? apiErrorMessage(remove.error, "Failed to delete the records.") : null}
      submit={{
        label: "Delete records",
        pending: remove.isPending,
        disabled: compacted || badBefore || typed !== topic.name,
        destructive: true,
      }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => remove.mutate(undefined, { onSuccess: onDeleted })}
    >
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
        <Field label="Partition" htmlFor={`${id}-partition`}>
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
        </Field>
        <Field label="Before offset" htmlFor={`${id}-before`}>
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
        </Field>
      </div>

      <TypeToConfirm id={`${id}-name`} name={topic.name} value={typed} onChange={setTyped} />
    </DialogForm>
  );
}
