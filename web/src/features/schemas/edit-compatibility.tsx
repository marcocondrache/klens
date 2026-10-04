import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Dialog } from "@/components/ui/dialog";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DialogForm, FormDialogContent } from "@/components/write-form";
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
  open,
  onOpenChange,
}: {
  cluster: string;
  subject: SubjectRow;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <FormDialogContent>
        <EditCompatibilityForm
          cluster={cluster}
          subject={subject}
          onSaved={() => onOpenChange(false)}
        />
      </FormDialogContent>
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
  const rule = LEVELS.find((entry) => entry.value === level)?.rule;

  const save = useMutation({
    mutationFn: (edit: EditSubject) =>
      patch(clusterPathname(cluster, "subjects", resourceId(subject.subject)), edit),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: keys.subjectRows(cluster), exact: true }),
  });

  return (
    <DialogForm
      title="Compatibility"
      description={
        <>
          The registry checks every new schema for{" "}
          <span className="font-mono">{subject.subject}</span> against this level. Schemas it
          already holds stay as they are.
        </>
      }
      error={
        save.isError ? apiErrorMessage(save.error, "Failed to set the compatibility level.") : null
      }
      submit={{ label: "Save", pending: save.isPending, disabled: level === subject.compatibility }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => save.mutate({ compatibility: level }, { onSuccess: onSaved })}
    >
      <Field>
        <FieldLabel htmlFor={`${id}-level`}>Level</FieldLabel>
        <Select
          items={LEVELS}
          value={level}
          onValueChange={(next) => {
            if (next !== null) setLevel(next);
          }}
        >
          <SelectTrigger
            id={`${id}-level`}
            className="w-full"
            aria-describedby={rule ? `${id}-level-hint` : undefined}
          >
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
        {rule ? <FieldDescription id={`${id}-level-hint`}>{rule}</FieldDescription> : null}
      </Field>
    </DialogForm>
  );
}
