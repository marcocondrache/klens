import { useId, useState, type ReactNode } from "react";
import { useMutation } from "@tanstack/react-query";

import { Dialog } from "@/components/ui/dialog";
import { DialogForm, FormDialogContent, TypeToConfirm } from "@/components/write-form";
import { apiErrorMessage } from "@/lib/api/client";

export function ConfirmDelete({
  open,
  onOpenChange,
  noun,
  name,
  consequence,
  onDelete,
  onDeleted,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  noun: string;
  name: string;
  consequence: ReactNode;
  onDelete: () => Promise<unknown>;
  onDeleted: () => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <FormDialogContent>
        <ConfirmForm
          noun={noun}
          name={name}
          consequence={consequence}
          onDelete={onDelete}
          onDeleted={() => {
            onOpenChange(false);
            onDeleted();
          }}
        />
      </FormDialogContent>
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

  return (
    <DialogForm
      title={`Delete ${noun}`}
      description={consequence}
      error={remove.isError ? apiErrorMessage(remove.error, `Failed to delete the ${noun}.`) : null}
      submit={{
        label: `Delete ${noun}`,
        pending: remove.isPending,
        disabled: typed !== name,
        destructive: true,
      }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => remove.mutate(undefined, { onSuccess: onDeleted })}
    >
      <TypeToConfirm id={`${id}-name`} name={name} value={typed} onChange={setTyped} />
    </DialogForm>
  );
}
