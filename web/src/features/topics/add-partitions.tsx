import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Dialog, DialogContent } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Field } from "@/components/field";
import { DialogForm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, post } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { AddPartitions, TopicDetail } from "@/lib/api/types";

const COUNT = /^[1-9]\d*$/;

export function AddPartitionsDialog({
  cluster,
  topic,
  open,
  onOpenChange,
}: {
  cluster: string;
  topic: TopicDetail;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <AddPartitionsForm cluster={cluster} topic={topic} onAdded={() => onOpenChange(false)} />
      </DialogContent>
    </Dialog>
  );
}

function AddPartitionsForm({
  cluster,
  topic,
  onAdded,
}: {
  cluster: string;
  topic: TopicDetail;
  onAdded: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const current = topic.partitions.length;
  const [count, setCount] = useState(String(current + 1));

  const add = useMutation({
    mutationFn: (request: AddPartitions) =>
      post(
        clusterPathname(cluster, "topics", encodeURIComponent(topic.name), "partitions"),
        request,
      ),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: keys.topic(cluster, topic.name), exact: true }),
  });

  const valid = COUNT.test(count) && Number(count) > current;

  return (
    <DialogForm
      title="Add partitions"
      description="Kafka cannot remove partitions later, and records with a key may land in a different partition than earlier records with the same key."
      error={add.isError ? apiErrorMessage(add.error, "Failed to add partitions.") : null}
      submit={{ label: "Add partitions", pending: add.isPending, disabled: !valid }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => add.mutate({ count: Number(count) }, { onSuccess: onAdded })}
    >
      <Field
        label="Partition count"
        htmlFor={`${id}-count`}
        hint={`${topic.name} has ${current} ${current === 1 ? "partition" : "partitions"} now.`}
      >
        <Input
          id={`${id}-count`}
          autoFocus
          inputMode="numeric"
          autoComplete="off"
          className="numeric w-32"
          value={count}
          aria-invalid={(count !== "" && !valid) || undefined}
          aria-describedby={`${id}-count-hint`}
          onChange={(event) => setCount(event.target.value.trim())}
        />
      </Field>
    </DialogForm>
  );
}
