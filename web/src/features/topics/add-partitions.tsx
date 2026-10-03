import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, PlusIcon } from "lucide-react";

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
import { Spinner } from "@/components/ui/spinner";
import { apiErrorMessage, clusterPathname, post } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { AddPartitions, TopicDetail } from "@/lib/api/types";

const COUNT = /^[1-9]\d*$/;

export function AddPartitionsDialog({ cluster, topic }: { cluster: string; topic: TopicDetail }) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button variant="outline" size="sm" />}>
        <PlusIcon data-icon="inline-start" />
        Add partitions
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <AddPartitionsForm cluster={cluster} topic={topic} onAdded={() => setOpen(false)} />
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
  const ready = valid && !add.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    add.mutate({ count: Number(count) }, { onSuccess: onAdded });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Add partitions</DialogTitle>
        <DialogDescription>
          Kafka cannot remove partitions later, and records with a key may land in a different
          partition than earlier records with the same key.
        </DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-count`}>Partition count</Label>
        <Input
          id={`${id}-count`}
          autoFocus
          inputMode="numeric"
          autoComplete="off"
          className="numeric w-32"
          value={count}
          aria-invalid={(count !== "" && !valid) || undefined}
          aria-describedby={`${id}-current`}
          onChange={(event) => setCount(event.target.value.trim())}
        />
        <p id={`${id}-current`} className="text-xs text-muted-foreground">
          {topic.name} has {current} {current === 1 ? "partition" : "partitions"} now.
        </p>
      </div>

      {add.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription>
            {apiErrorMessage(add.error, "Failed to add partitions.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={!ready}>
          {add.isPending ? <Spinner data-icon="inline-start" /> : null}
          Add partitions
        </Button>
      </DialogFooter>
    </form>
  );
}
