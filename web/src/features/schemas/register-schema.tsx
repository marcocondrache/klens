import { useId, useState, type FormEvent, type ReactElement, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, PlusIcon, XIcon } from "lucide-react";

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
import { Textarea } from "@/components/ui/textarea";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { IconButton } from "@/components/icon-button";
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

export function RegisterSchemaDialog({
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
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={trigger}>{children}</DialogTrigger>
      <DialogContent className="sm:max-w-2xl">
        <RegisterSchemaForm
          cluster={cluster}
          draft={draft}
          onRegistered={(subject, registered) => {
            setOpen(false);
            onRegistered(subject, registered);
          }}
        />
      </DialogContent>
    </Dialog>
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
  const ready =
    name !== "" && schema.trim() !== "" && !filled.some(badReference) && !register.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
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
        // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
        onSuccess: (registered) => onRegistered(name, registered),
      },
    );
  }

  function editReference(rowId: number, patch: Partial<ReferenceRow>) {
    setReferences((rows) => rows.map((row) => (row.id === rowId ? { ...row, ...patch } : row)));
  }

  return (
    <form className="grid min-w-0 gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>{draft.subject === null ? "Register schema" : "New version"}</DialogTitle>
        <DialogDescription>
          {draft.subject === null ? (
            "The registry stores the schema as version 1 of a new subject, or as the next version of an existing one."
          ) : (
            <>
              The registry checks the schema against the compatibility level of{" "}
              <span className="font-mono">{draft.subject}</span> and stores it as the next version.
              A schema the subject already holds keeps its version.
            </>
          )}
        </DialogDescription>
      </DialogHeader>

      {draft.subject === null ? (
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-subject`}>Subject</Label>
          <Input
            id={`${id}-subject`}
            autoFocus
            autoComplete="off"
            spellCheck={false}
            placeholder="orders-value"
            className="font-mono"
            value={subject}
            onChange={(event) => setSubject(event.target.value)}
          />
        </div>
      ) : null}

      <div className="grid gap-1.5">
        <div className="flex items-center justify-between gap-2">
          <Label htmlFor={`${id}-schema`}>Schema</Label>
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
        </div>
        <Textarea
          id={`${id}-schema`}
          autoFocus={draft.subject !== null}
          spellCheck={false}
          className="max-h-80 min-h-48 font-mono"
          value={schema}
          onChange={(event) => setSchema(event.target.value)}
        />
      </div>

      <div className="grid gap-1.5">
        <Label>References</Label>
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
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="justify-self-start"
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
      </div>

      {register.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(register.error, "Failed to register the schema.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={!ready}>
          {register.isPending ? <Spinner data-icon="inline-start" /> : null}
          Register
        </Button>
      </DialogFooter>
    </form>
  );
}
