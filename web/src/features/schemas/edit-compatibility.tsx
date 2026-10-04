import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, ShieldCheckIcon } from "lucide-react";

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
import { apiErrorMessage, clusterPathname, patch, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { EditSubject, SchemaCompatibility, SubjectRow } from "@/lib/api/types";

const LEVELS: { value: SchemaCompatibility; label: string; rule: string }[] = [
  {
    value: "BACKWARD",
    label: "Backward",
    rule: "Consumers on a new schema read data written with the version before it.",
  },
  {
    value: "BACKWARD_TRANSITIVE",
    label: "Backward transitive",
    rule: "Consumers on a new schema read data written with every earlier version.",
  },
  {
    value: "FORWARD",
    label: "Forward",
    rule: "Consumers on the version before a new schema read data written with it.",
  },
  {
    value: "FORWARD_TRANSITIVE",
    label: "Forward transitive",
    rule: "Consumers on every earlier version read data written with a new schema.",
  },
  {
    value: "FULL",
    label: "Full",
    rule: "A new schema is backward and forward compatible with the version before it.",
  },
  {
    value: "FULL_TRANSITIVE",
    label: "Full transitive",
    rule: "A new schema is backward and forward compatible with every earlier version.",
  },
  { value: "NONE", label: "None", rule: "The registry takes any new schema." },
];

export function EditCompatibilityDialog({
  cluster,
  subject,
}: {
  cluster: string;
  subject: SubjectRow;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button variant="outline" size="sm" />}>
        <ShieldCheckIcon data-icon="inline-start" />
        Compatibility
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <EditCompatibilityForm cluster={cluster} subject={subject} onSaved={() => setOpen(false)} />
      </DialogContent>
    </Dialog>
  );
}

function EditCompatibilityForm({
  cluster,
  subject,
  onSaved,
}: {
  cluster: string;
  subject: SubjectRow;
  onSaved: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [level, setLevel] = useState(subject.compatibility);

  const save = useMutation({
    mutationFn: (edit: EditSubject) =>
      patch(clusterPathname(cluster, "subjects", resourceId(subject.subject)), edit),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: keys.subjectRows(cluster), exact: true }),
  });

  const ready = level !== subject.compatibility && !save.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    save.mutate({ compatibility: level }, { onSuccess: onSaved });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Compatibility</DialogTitle>
        <DialogDescription>
          The registry checks every new schema for{" "}
          <span className="font-mono">{subject.subject}</span> against this level. Schemas it
          already holds stay as they are.
        </DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-level`}>Level</Label>
        <Select
          items={LEVELS}
          value={level}
          onValueChange={(next) => {
            if (next !== null) setLevel(next);
          }}
        >
          <SelectTrigger id={`${id}-level`} className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LEVELS.map((entry) => (
              <SelectItem key={entry.value} value={entry.value}>
                {entry.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <p className="text-sm text-muted-foreground">
          {LEVELS.find((entry) => entry.value === level)?.rule}
        </p>
      </div>

      {save.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(save.error, "Failed to set the compatibility level.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={!ready}>
          {save.isPending ? <Spinner data-icon="inline-start" /> : null}
          Save
        </Button>
      </DialogFooter>
    </form>
  );
}
