import { useId, useState, type FormEvent, type ReactNode } from "react";
import { useMutation } from "@tanstack/react-query";
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
import { apiErrorMessage } from "@/lib/api/client";

export function ConfirmDelete({
  noun,
  name,
  consequence,
  onDelete,
  onDeleted,
}: {
  noun: string;
  name: string;
  consequence: ReactNode;
  onDelete: () => Promise<unknown>;
  onDeleted: () => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button variant="destructive" />}>
        <Trash2Icon data-icon="inline-start" />
        Delete {noun}
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <ConfirmForm
          noun={noun}
          name={name}
          consequence={consequence}
          onDelete={onDelete}
          onDeleted={() => {
            setOpen(false);
            onDeleted();
          }}
        />
      </DialogContent>
    </Dialog>
  );
}

function ConfirmForm({
  noun,
  name,
  consequence,
  onDelete,
  onDeleted,
}: {
  noun: string;
  name: string;
  consequence: ReactNode;
  onDelete: () => Promise<unknown>;
  onDeleted: () => void;
}) {
  const id = useId();
  const [typed, setTyped] = useState("");
  const remove = useMutation({ mutationFn: onDelete });
  const ready = typed === name && !remove.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    remove.mutate(undefined, { onSuccess: onDeleted });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Delete {noun}</DialogTitle>
        <DialogDescription>{consequence}</DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-name`}>
          Type <span className="font-mono">{name}</span> to confirm
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
            {apiErrorMessage(remove.error, `Failed to delete the ${noun}.`)}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" variant="destructive" disabled={!ready}>
          {remove.isPending ? <Spinner data-icon="inline-start" /> : null}
          Delete {noun}
        </Button>
      </DialogFooter>
    </form>
  );
}
