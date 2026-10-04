import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, Trash2Icon } from "lucide-react";

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
import { Switch } from "@/components/ui/switch";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { apiErrorMessage, clusterPathname, del, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { SubjectRow } from "@/lib/api/types";

const SUBJECT = "subject";

/** `onDeleted` gets the version left on screen, or null once the subject is gone. */
export function DeleteSchemaDialog({
  cluster,
  subject,
  version,
  onDeleted,
}: {
  cluster: string;
  subject: SubjectRow;
  version: number;
  onDeleted: (remaining: number | null) => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button variant="outline" size="sm" />}>
        <Trash2Icon data-icon="inline-start" />
        Delete
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DeleteSchemaForm
          cluster={cluster}
          subject={subject}
          version={version}
          onDeleted={(remaining) => {
            setOpen(false);
            onDeleted(remaining);
          }}
        />
      </DialogContent>
    </Dialog>
  );
}

function DeleteSchemaForm({
  cluster,
  subject,
  version,
  onDeleted,
}: {
  cluster: string;
  subject: SubjectRow;
  version: number;
  onDeleted: (remaining: number | null) => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [scope, setScope] = useState(String(version));
  const [permanent, setPermanent] = useState(false);
  const [typed, setTyped] = useState("");
  const whole = scope === SUBJECT;
  const remaining = whole ? [] : subject.versions.filter((kept) => kept !== version);

  const remove = useMutation({
    mutationFn: () =>
      del(clusterPathname(cluster, "subjects", resourceId(subject.subject)), {
        version: whole ? undefined : version,
        permanent,
      }),
    // Not returned, since the refetch can drop the subject and unmount this form first.
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: keys.subjectRows(cluster), exact: true });
      void queryClient.invalidateQueries({
        queryKey: keys.subjectVersions(cluster, subject.subject),
      });
    },
  });

  const ready = typed === subject.subject && !remove.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    remove.mutate(undefined, { onSuccess: () => onDeleted(remaining.at(-1) ?? null) });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Delete schema</DialogTitle>
        <DialogDescription>
          {whole ? "Every version" : `Version ${version}`} of{" "}
          <span className="font-mono">{subject.subject}</span> leaves the registry.{" "}
          {permanent
            ? "Its schema IDs are gone for good, and records that carry them no longer decode."
            : "The registry keeps its schema IDs, so records that carry them still decode."}
        </DialogDescription>
      </DialogHeader>

      <ToggleGroup
        value={[scope]}
        onValueChange={(next) => {
          const picked = next[0];
          if (picked) setScope(picked);
        }}
        variant="outline"
        size="sm"
        spacing={0}
        className="justify-self-start"
        aria-label="What to delete"
      >
        <ToggleGroupItem value={String(version)}>Version {version}</ToggleGroupItem>
        <ToggleGroupItem value={SUBJECT}>All versions</ToggleGroupItem>
      </ToggleGroup>

      <Label className="flex items-center gap-2 font-normal">
        <Switch size="sm" checked={permanent} onCheckedChange={setPermanent} />
        Delete permanently
      </Label>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-name`}>
          Type <span className="font-mono">{subject.subject}</span> to confirm
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
          <AlertDescription className="break-words">
            {apiErrorMessage(remove.error, "Failed to delete the schema.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" variant="destructive" disabled={!ready}>
          {remove.isPending ? <Spinner data-icon="inline-start" /> : null}
          Delete
        </Button>
      </DialogFooter>
    </form>
  );
}
