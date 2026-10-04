import type { FormEvent } from "react";
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
import { Spinner } from "@/components/ui/spinner";
import { IconButton } from "@/components/icon-button";
import { apiErrorMessage, clusterPathname, del } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { Acl } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";

export function DeleteAclButton({ handle, acl }: { handle: DialogHandle<Acl>; acl: Acl }) {
  return (
    <DialogTrigger
      handle={handle}
      payload={acl}
      render={<IconButton label="Delete ACL" tooltip="Delete" />}
    >
      <Trash2Icon />
    </DialogTrigger>
  );
}

export function DeleteAclDialog({
  cluster,
  handle,
}: {
  cluster: string;
  handle: DialogHandle<Acl>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <DialogContent className="sm:max-w-md">
          {payload ? (
            <DeleteAclForm cluster={cluster} acl={payload} onDeleted={() => handle.close()} />
          ) : null}
        </DialogContent>
      )}
    </Dialog>
  );
}

function DeleteAclForm({
  cluster,
  acl,
  onDeleted,
}: {
  cluster: string;
  acl: Acl;
  onDeleted: () => void;
}) {
  const queryClient = useQueryClient();
  const remove = useMutation({
    mutationFn: () => del(clusterPathname(cluster, "acls"), acl),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: keys.acls(cluster), exact: true }),
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    if (remove.isPending) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    remove.mutate(undefined, { onSuccess: onDeleted });
  }

  const resource = formatEnumLabel(acl.resourceType).toLowerCase();
  const operation =
    acl.operation === "ALL" ? "every operation" : `the ${formatEnumLabel(acl.operation)} operation`;

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Delete ACL</DialogTitle>
        <DialogDescription>
          This binding {acl.permission === "ALLOW" ? "allows" : "denies"}{" "}
          <span className="font-mono">{acl.principal}</span> from{" "}
          <span className="font-mono">{acl.host}</span> {operation} on{" "}
          {acl.patternType === "PREFIXED" ? `every ${resource} starting with` : `the ${resource}`}{" "}
          <span className="font-mono">{acl.resourceName}</span>
        </DialogDescription>
      </DialogHeader>

      {remove.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(remove.error, "Failed to delete the ACL.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" variant="destructive" disabled={remove.isPending}>
          {remove.isPending ? <Spinner data-icon="inline-start" /> : null}
          Delete ACL
        </Button>
      </DialogFooter>
    </form>
  );
}
