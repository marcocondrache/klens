import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Dialog } from "@/components/ui/dialog";
import { Field, FieldLabel, FieldTitle } from "@/components/ui/field";
import { Switch } from "@/components/ui/switch";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { DialogForm, FormDialogContent, TypeToConfirm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, del, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { SubjectRow } from "@/lib/api/types";

const SUBJECT = "subject";

/** `onDeleted` gets the version left on screen, or null once the subject is gone. */
export function DeleteSchemaDialog({
  cluster,
  subject,
  version,
  open,
  onOpenChange,
  onDeleted,
}: {
  cluster: string;
  subject: SubjectRow;
  version: number;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onDeleted: (remaining: number | null) => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <FormDialogContent>
        <DeleteSchemaForm
          cluster={cluster}
          subject={subject}
          version={version}
          onDeleted={(remaining) => {
            onOpenChange(false);
            onDeleted(remaining);
          }}
        />
      </FormDialogContent>
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

  return (
    <DialogForm
      title="Delete schema"
      description={
        <>
          {whole ? "Every version" : `Version ${version}`} of{" "}
          <span className="font-mono">{subject.subject}</span> leaves the registry.{" "}
          {permanent
            ? "Its schema IDs are gone for good, and records that carry them no longer decode."
            : "The registry keeps its schema IDs, so records that carry them still decode."}
        </>
      }
      error={remove.isError ? apiErrorMessage(remove.error, "Failed to delete the schema.") : null}
      submit={{
        label: "Delete",
        pending: remove.isPending,
        disabled: typed !== subject.subject,
        destructive: true,
      }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() =>
        remove.mutate(undefined, { onSuccess: () => onDeleted(remaining.at(-1) ?? null) })
      }
    >
      <Field>
        <FieldTitle id={`${id}-scope`}>What to delete</FieldTitle>
        <div>
          <ToggleGroup
            value={[scope]}
            onValueChange={(next) => {
              const picked = next[0];
              if (picked) setScope(picked);
            }}
            variant="outline"
            size="sm"
            spacing={0}
            aria-labelledby={`${id}-scope`}
          >
            <ToggleGroupItem value={String(version)}>Version {version}</ToggleGroupItem>
            <ToggleGroupItem value={SUBJECT}>All versions</ToggleGroupItem>
          </ToggleGroup>
        </div>
      </Field>

      <Field orientation="horizontal" className="w-fit">
        <Switch
          id={`${id}-permanent`}
          size="sm"
          checked={permanent}
          onCheckedChange={setPermanent}
        />
        <FieldLabel htmlFor={`${id}-permanent`} className="text-sm font-normal text-foreground">
          Delete permanently
        </FieldLabel>
      </Field>

      <TypeToConfirm id={`${id}-name`} name={subject.subject} value={typed} onChange={setTyped} />
    </DialogForm>
  );
}
