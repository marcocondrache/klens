import { useState, type FormEvent } from "react";
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
  type DialogHandle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Spinner } from "@/components/ui/spinner";
import { IconButton } from "@/components/icon-button";
import { apiErrorMessage, clusterPathname, del, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ScramMechanism, ScramUser } from "@/lib/api/types";

import { MechanismToggle } from "./mechanism-toggle";
import { MECHANISM_LABEL } from "./users-columns";

export function DeleteCredentialButton({
  handle,
  user,
}: {
  handle: DialogHandle<ScramUser>;
  user: ScramUser;
}) {
  return (
    <DialogTrigger
      handle={handle}
      payload={user}
      render={<IconButton label={`Delete a credential of ${user.name}`} tooltip="Delete" />}
    >
      <Trash2Icon />
    </DialogTrigger>
  );
}

export function DeleteCredentialDialog({
  cluster,
  handle,
}: {
  cluster: string;
  handle: DialogHandle<ScramUser>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <DialogContent className="sm:max-w-md">
          {payload ? (
            <DeleteCredentialForm
              cluster={cluster}
              user={payload}
              onDeleted={() => handle.close()}
            />
          ) : null}
        </DialogContent>
      )}
    </Dialog>
  );
}

function DeleteCredentialForm({
  cluster,
  user,
  onDeleted,
}: {
  cluster: string;
  user: ScramUser;
  onDeleted: () => void;
}) {
  const queryClient = useQueryClient();
  const held = user.credentials.map((credential) => credential.mechanism);
  const [mechanism, setMechanism] = useState<ScramMechanism>(held[0] ?? "SHA512");
  const remove = useMutation({
    mutationFn: (mechanism: ScramMechanism) =>
      del(clusterPathname(cluster, "scram-users", resourceId(user.name)), { mechanism }),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: keys.scramUsers(cluster), exact: true }),
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    if (remove.isPending) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    remove.mutate(mechanism, { onSuccess: onDeleted });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Delete credential</DialogTitle>
        <DialogDescription>
          <span className="font-mono">{user.name}</span> can no longer log in with{" "}
          {MECHANISM_LABEL[mechanism]}
          {held.length === 1 ? ", its only SCRAM credential." : "."}
        </DialogDescription>
      </DialogHeader>

      {held.length > 1 ? (
        <div className="grid gap-1.5">
          <Label>Mechanism</Label>
          <MechanismToggle value={mechanism} options={held} onChange={setMechanism} />
        </div>
      ) : null}

      {remove.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(remove.error, "Failed to delete the credential.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" variant="destructive" disabled={remove.isPending}>
          {remove.isPending ? <Spinner data-icon="inline-start" /> : null}
          Delete credential
        </Button>
      </DialogFooter>
    </form>
  );
}
