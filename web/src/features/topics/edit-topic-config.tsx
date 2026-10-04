import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, PencilIcon } from "lucide-react";

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
  type DialogHandle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Spinner } from "@/components/ui/spinner";
import { IconButton } from "@/components/icon-button";
import { apiErrorMessage, clusterPathname, patch } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ConfigEntry, EditConfigs } from "@/lib/api/types";

export function EditTopicConfigButton({
  handle,
  entry,
}: {
  handle: DialogHandle<ConfigEntry>;
  entry: ConfigEntry;
}) {
  return (
    <DialogTrigger
      handle={handle}
      payload={entry}
      render={<IconButton label={`Edit ${entry.name}`} tooltip="Edit" />}
    >
      <PencilIcon />
    </DialogTrigger>
  );
}

export function EditTopicConfigDialog({
  cluster,
  topic,
  handle,
}: {
  cluster: string;
  topic: string;
  handle: DialogHandle<ConfigEntry>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <DialogContent className="sm:max-w-md">
          {payload ? (
            <EditTopicConfigForm
              cluster={cluster}
              topic={topic}
              entry={payload}
              onEdited={() => handle.close()}
            />
          ) : null}
        </DialogContent>
      )}
    </Dialog>
  );
}

function EditTopicConfigForm({
  cluster,
  topic,
  entry,
  onEdited,
}: {
  cluster: string;
  topic: string;
  entry: ConfigEntry;
  onEdited: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [value, setValue] = useState(entry.sensitive ? "" : (entry.value ?? ""));
  const overridden = entry.source === "DYNAMIC_TOPIC_CONFIG";

  const edit = useMutation({
    mutationFn: (change: EditConfigs) =>
      patch(clusterPathname(cluster, "topics", encodeURIComponent(topic), "configs"), change),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: keys.topicConfigs(cluster, topic) }),
  });
  const resetting = edit.isPending && edit.variables.reset.length > 0;
  const unchanged = overridden && !entry.sensitive && value === entry.value;

  function save(change: EditConfigs) {
    if (edit.isPending) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    edit.mutate(change, { onSuccess: onEdited });
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    if (unchanged) return;
    save({ set: { [entry.name]: value }, reset: [] });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Edit config</DialogTitle>
        <DialogDescription>
          {overridden ? (
            <>
              <span className="font-mono">{topic}</span> overrides the broker&apos;s value. Remove
              the override to follow the broker again.
            </>
          ) : (
            <>
              Saving sets an override on <span className="font-mono">{topic}</span> in place of the
              broker&apos;s value.
            </>
          )}
        </DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-value`} className="font-mono">
          {entry.name}
        </Label>
        <Input
          id={`${id}-value`}
          autoFocus
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          placeholder={entry.sensitive ? "Hidden" : undefined}
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
      </div>

      {edit.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription>
            {apiErrorMessage(edit.error, "Failed to change the config.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        {overridden ? (
          <Button
            type="button"
            variant="outline"
            className="sm:mr-auto"
            disabled={edit.isPending}
            onClick={() => save({ set: {}, reset: [entry.name] })}
          >
            {resetting ? <Spinner data-icon="inline-start" /> : null}
            Remove override
          </Button>
        ) : null}
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={unchanged || edit.isPending}>
          {edit.isPending && !resetting ? <Spinner data-icon="inline-start" /> : null}
          Save
        </Button>
      </DialogFooter>
    </form>
  );
}
