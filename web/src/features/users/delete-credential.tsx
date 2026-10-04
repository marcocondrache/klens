import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Dialog, type DialogHandle } from "@/components/ui/dialog";
import { Field } from "@/components/field";
import { DialogForm, FormDialogContent } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, del, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ScramMechanism, ScramUser } from "@/lib/api/types";

import { MechanismToggle } from "./mechanism-toggle";
import { MECHANISM_LABEL } from "./users-columns";

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
        <FormDialogContent>
          {payload ? (
            <DeleteCredentialForm
              cluster={cluster}
              user={payload}
              onDeleted={() => handle.close()}
            />
          ) : null}
        </FormDialogContent>
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

  return (
    <DialogForm
      title="Delete credential"
      description={
        <>
          <span className="font-mono">{user.name}</span> can no longer log in with{" "}
          {MECHANISM_LABEL[mechanism]}
          {held.length === 1 ? ", its only SCRAM credential." : "."}
        </>
      }
      error={
        remove.isError ? apiErrorMessage(remove.error, "Failed to delete the credential.") : null
      }
      submit={{ label: "Delete credential", pending: remove.isPending, destructive: true }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => remove.mutate(mechanism, { onSuccess: onDeleted })}
    >
      {held.length > 1 ? (
        <Field label="Mechanism">
          <MechanismToggle value={mechanism} options={held} onChange={setMechanism} />
        </Field>
      ) : null}
    </DialogForm>
  );
}
