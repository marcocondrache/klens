import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, ListXIcon } from "lucide-react";

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
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import { committedTopics } from "@/features/groups/group-state";
import { apiErrorMessage, clusterPathname, del, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { GroupDetail } from "@/lib/api/types";

export function DeleteOffsetsDialog({ cluster, group }: { cluster: string; group: GroupDetail }) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button variant="outline" size="sm" />}>
        <ListXIcon data-icon="inline-start" />
        Delete offsets
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DeleteOffsetsForm cluster={cluster} group={group} onDeleted={() => setOpen(false)} />
      </DialogContent>
    </Dialog>
  );
}

function DeleteOffsetsForm({
  cluster,
  group,
  onDeleted,
}: {
  cluster: string;
  group: GroupDetail;
  onDeleted: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const topics = committedTopics(group);
  const consumed = new Set(
    group.members.flatMap((member) => member.assignments.map((assignment) => assignment.topic)),
  );
  const [topic, setTopic] = useState(
    () => topics.find((name) => !consumed.has(name)) ?? topics[0] ?? "",
  );

  const remove = useMutation({
    mutationFn: () =>
      del(clusterPathname(cluster, "group-offsets", resourceId(group.id)), { topic }),
    onSuccess: () =>
      Promise.all([
        queryClient.invalidateQueries({ queryKey: keys.group(cluster, group.id), exact: true }),
        queryClient.invalidateQueries({ queryKey: keys.groupRows(cluster), exact: true }),
      ]),
  });

  const items = topics.map((name) => ({ value: name, label: name }));
  const ready = topic !== "" && !consumed.has(topic) && !remove.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    remove.mutate(undefined, { onSuccess: onDeleted });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Delete offsets</DialogTitle>
        <DialogDescription>
          This deletes the offsets <span className="font-mono">{group.id}</span> committed on{" "}
          <span className="font-mono">{topic}</span>. Its consumers start from their reset policy if
          they read the topic again.
        </DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-topic`}>Topic</Label>
        <Select
          items={items}
          value={topic}
          onValueChange={(next) => {
            if (next !== null) setTopic(next);
          }}
        >
          <SelectTrigger id={`${id}-topic`} className="w-full font-mono">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {items.map((item) => (
              <SelectItem key={item.value} value={item.value} className="font-mono">
                {item.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {consumed.has(topic) ? (
        <Alert>
          <CircleAlertIcon />
          <AlertDescription>
            A member of the group still consumes this topic. Kafka only deletes the offsets of a
            topic the group no longer reads.
          </AlertDescription>
        </Alert>
      ) : null}

      {remove.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription>
            {apiErrorMessage(remove.error, "Failed to delete the offsets.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" variant="destructive" disabled={!ready}>
          {remove.isPending ? <Spinner data-icon="inline-start" /> : null}
          Delete offsets
        </Button>
      </DialogFooter>
    </form>
  );
}
