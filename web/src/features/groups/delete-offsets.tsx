import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon } from "lucide-react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Dialog } from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Field } from "@/components/field";
import { DialogForm, FormDialogContent } from "@/components/write-form";
import { committedTopics } from "@/features/groups/group-state";
import { apiErrorMessage, clusterPathname, del, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { GroupDetail } from "@/lib/api/types";

export function DeleteOffsetsDialog({
  cluster,
  group,
  open,
  onOpenChange,
}: {
  cluster: string;
  group: GroupDetail;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <FormDialogContent>
        <DeleteOffsetsForm cluster={cluster} group={group} onDeleted={() => onOpenChange(false)} />
      </FormDialogContent>
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

  return (
    <DialogForm
      title="Delete offsets"
      description={
        <>
          This deletes the offsets <span className="font-mono">{group.id}</span> committed on{" "}
          <span className="font-mono">{topic}</span>. Its consumers start from their reset policy if
          they read the topic again.
        </>
      }
      error={remove.isError ? apiErrorMessage(remove.error, "Failed to delete the offsets.") : null}
      submit={{
        label: "Delete offsets",
        pending: remove.isPending,
        disabled: topic === "" || consumed.has(topic),
        destructive: true,
      }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => remove.mutate(undefined, { onSuccess: onDeleted })}
    >
      <Field label="Topic" htmlFor={`${id}-topic`}>
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
      </Field>

      {consumed.has(topic) ? (
        <Alert>
          <CircleAlertIcon />
          <AlertDescription>
            A member of the group still consumes this topic. Kafka only deletes the offsets of a
            topic the group no longer reads.
          </AlertDescription>
        </Alert>
      ) : null}
    </DialogForm>
  );
}
