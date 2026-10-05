import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Dialog } from "@/components/ui/dialog";
import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { DialogForm, FormDialogContent } from "@/components/write-form";
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
      <FormDialogContent>
        <AddPartitionsForm cluster={cluster} topic={topic} onAdded={() => onOpenChange(false)} />
      </FormDialogContent>
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
  const invalid = count !== "" && !valid;

  return (
    <DialogForm
      title="Add partitions"
      description="Kafka cannot remove partitions later, and records with a key may land in a different partition than earlier records with the same key."
      error={add.isError ? apiErrorMessage(add.error, "Failed to add partitions.") : null}
      submit={{ label: "Add partitions", pending: add.isPending, disabled: !valid }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => add.mutate({ count: Number(count) }, { onSuccess: onAdded })}
    >
      <Field data-invalid={invalid || undefined}>
        <FieldLabel htmlFor={`${id}-count`}>Partition count</FieldLabel>
        <Input
          id={`${id}-count`}
          data-autofocus
          inputMode="numeric"
          autoComplete="off"
          className="numeric max-w-32"
          value={count}
          aria-invalid={invalid || undefined}
          aria-describedby={invalid ? `${id}-count-hint ${id}-count-error` : `${id}-count-hint`}
          onChange={(event) => setCount(event.target.value.trim())}
        />
        <FieldDescription id={`${id}-count-hint`}>
          {topic.name} has {current} {current === 1 ? "partition" : "partitions"} now.
        </FieldDescription>
        {invalid ? (
          <FieldError id={`${id}-count-error`}>
            Enter more than {current} {current === 1 ? "partition" : "partitions"}.
          </FieldError>
        ) : null}
      </Field>
    </DialogForm>
  );
}
