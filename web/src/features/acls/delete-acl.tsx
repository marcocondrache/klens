import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Dialog, type DialogHandle } from "@/components/ui/dialog";
import { DialogForm, FormDialogContent } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, del } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { Acl } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";

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
        <FormDialogContent>
          {payload ? (
            <DeleteAclForm cluster={cluster} acl={payload} onDeleted={() => handle.close()} />
          ) : null}
        </FormDialogContent>
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

  const resource = formatEnumLabel(acl.resourceType).toLowerCase();
  const operation =
    acl.operation === "ALL" ? "every operation" : `the ${formatEnumLabel(acl.operation)} operation`;

  return (
    <DialogForm
      title="Delete ACL"
      description={
        <>
          This binding {acl.permission === "ALLOW" ? "allows" : "denies"}{" "}
          <span className="font-mono">{acl.principal}</span> from{" "}
          <span className="font-mono">{acl.host}</span> {operation} on{" "}
          {acl.patternType === "PREFIXED" ? `every ${resource} starting with` : `the ${resource}`}{" "}
          <span className="font-mono">{acl.resourceName}</span>
        </>
      }
      error={remove.isError ? apiErrorMessage(remove.error, "Failed to delete the ACL.") : null}
      submit={{ label: "Delete ACL", pending: remove.isPending, destructive: true }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => remove.mutate(undefined, { onSuccess: onDeleted })}
    />
  );
}
