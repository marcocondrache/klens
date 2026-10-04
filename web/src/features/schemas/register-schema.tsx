import { useId, useState, type ReactElement, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { PlusIcon, XIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { Textarea } from "@/components/ui/textarea";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Field, FieldCount } from "@/components/field";
import { IconButton } from "@/components/icon-button";
import { FormSheetContent, SheetForm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, postAndRead, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type {
  RegisterSchema,
  RegisteredVersion,
  SchemaReference,
  SchemaType,
} from "@/lib/api/types";

const VERSION = /^[1-9]\d*$/;

const TYPES: { value: SchemaType; label: string }[] = [
  { value: "AVRO", label: "Avro" },
  { value: "JSON", label: "JSON" },
  { value: "PROTOBUF", label: "Protobuf" },
];

/** A null `subject` asks for one, which registers a new subject. */
export type SchemaDraft = {
  subject: string | null;
  type: SchemaType;
  schema: string;
  references: SchemaReference[];
};

export const NEW_SUBJECT: SchemaDraft = {
  subject: null,
  type: "AVRO",
  schema: "",
  references: [],
};

type ReferenceRow = { id: number; name: string; subject: string; version: string };

export function RegisterSchemaSheet({
  cluster,
  draft,
  trigger,
  children,
  onRegistered,
}: {
  cluster: string;
  draft: SchemaDraft;
  trigger: ReactElement;
  children: ReactNode;
  onRegistered: (subject: string, registered: RegisteredVersion) => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger render={trigger}>{children}</SheetTrigger>
      <FormSheetContent wide>
        <RegisterSchemaForm
          cluster={cluster}
          draft={draft}
          onRegistered={(subject, registered) => {
            setOpen(false);
            onRegistered(subject, registered);
          }}
        />
      </FormSheetContent>
    </Sheet>
  );
}

function RegisterSchemaForm({
  cluster,
  draft,
  onRegistered,
}: {
  cluster: string;
  draft: SchemaDraft;
  onRegistered: (subject: string, registered: RegisteredVersion) => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [subject, setSubject] = useState(draft.subject ?? "");
  const [type, setType] = useState(draft.type);
  const [schema, setSchema] = useState(draft.schema);
  const [references, setReferences] = useState<ReferenceRow[]>(() =>
    draft.references.map((reference, index) => ({
      id: index + 1,
      ...reference,
      version: String(reference.version),
    })),
  );

  const register = useMutation({
    mutationFn: ({ subject, request }: { subject: string; request: RegisterSchema }) =>
      postAndRead<RegisteredVersion>(
        clusterPathname(cluster, "subjects", resourceId(subject)),
        request,
      ),
    onSuccess: (_, { subject }) =>
      Promise.all([
        queryClient.invalidateQueries({ queryKey: keys.subjectRows(cluster), exact: true }),
        queryClient.invalidateQueries({ queryKey: keys.subjectVersions(cluster, subject) }),
      ]),
  });

  const filled = references.filter(
    (row) => row.name !== "" || row.subject !== "" || row.version !== "",
  );
  const badReference = (row: ReferenceRow) =>
    (row.name !== "" || row.subject !== "" || row.version !== "") &&
    (row.name.trim() === "" || row.subject.trim() === "" || !VERSION.test(row.version));
  const name = subject.trim();

  function submit() {
    register.mutate(
      {
        subject: name,
        request: {
          type,
          schema,
          references: filled.map((row) => ({
            name: row.name.trim(),
            subject: row.subject.trim(),
            version: Number(row.version),
          })),
        },
      },
      {
        // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
        onSuccess: (registered) => onRegistered(name, registered),
      },
    );
  }

  function editReference(rowId: number, patch: Partial<ReferenceRow>) {
    setReferences((rows) => rows.map((row) => (row.id === rowId ? { ...row, ...patch } : row)));
  }

  return (
    <SheetForm
      title={draft.subject === null ? "Register schema" : "New version"}
      description={
        draft.subject === null ? (
          "The registry stores the schema as version 1 of a new subject, or as the next version of an existing one."
        ) : (
          <>
            The registry checks the schema against the compatibility level of{" "}
            <span className="font-mono">{draft.subject}</span> and stores it as the next version. A
            schema the subject already holds keeps its version.
          </>
        )
      }
      error={
        register.isError ? apiErrorMessage(register.error, "Failed to register the schema.") : null
      }
      submit={{
        label: "Register",
        pending: register.isPending,
        disabled: name === "" || schema.trim() === "" || filled.some(badReference),
      }}
      onSubmit={submit}
    >
      {draft.subject === null ? (
        <Field label="Subject" htmlFor={`${id}-subject`}>
          <Input
            id={`${id}-subject`}
            data-autofocus
            autoComplete="off"
            spellCheck={false}
            placeholder="orders-value"
            className="font-mono"
            value={subject}
            onChange={(event) => setSubject(event.target.value)}
          />
        </Field>
      ) : null}

      <Field
        label="Schema"
        htmlFor={`${id}-schema`}
        className="flex min-h-48 flex-1 flex-col"
        action={
          <ToggleGroup
            value={[type]}
            onValueChange={(next) => {
              const picked = next[0] as SchemaType | undefined;
              if (picked) setType(picked);
            }}
            variant="outline"
            size="sm"
            spacing={0}
            className="[&_[data-slot=toggle-group-item]]:h-6 [&_[data-slot=toggle-group-item]]:px-2 [&_[data-slot=toggle-group-item]]:text-xs"
            aria-label="Schema type"
          >
            {TYPES.map((entry) => (
              <ToggleGroupItem key={entry.value} value={entry.value}>
                {entry.label}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        }
      >
        <Textarea
          id={`${id}-schema`}
          data-autofocus={draft.subject !== null || undefined}
          spellCheck={false}
          className="min-h-0 flex-1 resize-none field-sizing-fixed bg-subtle px-3 py-2.5 font-mono leading-relaxed md:text-sm dark:bg-subtle"
          value={schema}
          onChange={(event) => setSchema(event.target.value)}
        />
      </Field>

      <Field
        label={
          <>
            References
            <FieldCount value={references.length} />
          </>
        }
        action={
          <Button
            type="button"
            variant="ghost"
            size="xs"
            className="text-muted-foreground"
            onClick={() =>
              setReferences((rows) => [
                ...rows,
                { id: (rows.at(-1)?.id ?? 0) + 1, name: "", subject: "", version: "" },
              ])
            }
          >
            <PlusIcon data-icon="inline-start" />
            Add reference
          </Button>
        }
        className="shrink-0"
      >
        {references.length === 0 ? (
          <p className="text-sm text-muted-foreground">No references.</p>
        ) : null}
        {references.map((row) => (
          <div
            key={row.id}
            className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_5rem_auto] gap-2"
          >
            <Input
              aria-label="Reference name"
              autoComplete="off"
              spellCheck={false}
              placeholder="common.proto"
              className="font-mono"
              value={row.name}
              aria-invalid={(badReference(row) && row.name.trim() === "") || undefined}
              onChange={(event) => editReference(row.id, { name: event.target.value })}
            />
            <Input
              aria-label="Reference subject"
              autoComplete="off"
              spellCheck={false}
              placeholder="Subject"
              className="font-mono"
              value={row.subject}
              aria-invalid={(badReference(row) && row.subject.trim() === "") || undefined}
              onChange={(event) => editReference(row.id, { subject: event.target.value })}
            />
            <Input
              aria-label="Reference version"
              inputMode="numeric"
              autoComplete="off"
              placeholder="Version"
              className="numeric"
              value={row.version}
              aria-invalid={(badReference(row) && !VERSION.test(row.version)) || undefined}
              onChange={(event) => editReference(row.id, { version: event.target.value.trim() })}
            />
            <IconButton
              label="Remove reference"
              size="icon"
              onClick={() => setReferences((rows) => rows.filter((other) => other.id !== row.id))}
            >
              <XIcon />
            </IconButton>
          </div>
        ))}
      </Field>
    </SheetForm>
  );
}
