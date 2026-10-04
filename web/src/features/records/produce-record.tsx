import { useId, useState, type FormEvent, type ReactElement, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, CircleCheckIcon, PlusIcon, XIcon } from "lucide-react";

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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import { Textarea } from "@/components/ui/textarea";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { IconButton } from "@/components/icon-button";
import { useSubjectRows } from "@/lib/api/catalog";
import { apiErrorMessage, clusterPathname, postAndRead } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type {
  KafkaRecord,
  ProduceRecord,
  ProducedRecord,
  RecordPayload,
  SubjectRow,
  TopicDetail,
} from "@/lib/api/types";

import { SchemaPicker } from "./schema-picker";

type Encoding = RecordPayload["encoding"] | "NULL";

type PayloadDraft = { encoding: Encoding; data: string; schemaId: number | null };

type HeaderRow = { id: number; key: string; value: string };

export type RecordDraft = {
  partition: number | null;
  key: PayloadDraft;
  value: PayloadDraft;
  headers: Omit<HeaderRow, "id">[];
};

const BLANK: RecordDraft = {
  partition: null,
  key: { encoding: "NULL", data: "", schemaId: null },
  value: { encoding: "TEXT", data: "", schemaId: null },
  headers: [],
};

const ANY_PARTITION = "any";

export function duplicateDraft(record: KafkaRecord): RecordDraft | null {
  if (!record.verbatim) return null;
  return {
    partition: record.partition,
    key: textDraft(record.key),
    value: textDraft(record.value),
    headers: record.headers,
  };
}

function textDraft(text: string | null): PayloadDraft {
  return { encoding: text === null ? "NULL" : "TEXT", data: text ?? "", schemaId: null };
}

/**
 * Without `onProduced` the dialog stays open and says where Kafka stored the record.
 * A null `trigger` hides the button but keeps an open dialog and its edits.
 */
export function ProduceRecordDialog({
  cluster,
  topic,
  draft = BLANK,
  trigger,
  children,
  onProduced,
}: {
  cluster: string;
  topic: TopicDetail;
  draft?: RecordDraft;
  trigger: ReactElement | null;
  children: ReactNode;
  onProduced?: (record: ProducedRecord) => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      {trigger ? <DialogTrigger render={trigger}>{children}</DialogTrigger> : null}
      <DialogContent className="sm:max-w-lg">
        <ProduceRecordForm
          cluster={cluster}
          topic={topic}
          draft={draft}
          onProduced={
            onProduced &&
            ((record) => {
              setOpen(false);
              onProduced(record);
            })
          }
        />
      </DialogContent>
    </Dialog>
  );
}

function ProduceRecordForm({
  cluster,
  topic,
  draft,
  onProduced,
}: {
  cluster: string;
  topic: TopicDetail;
  draft: RecordDraft;
  onProduced?: (record: ProducedRecord) => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const { data: subjects } = useSubjectRows(cluster);
  const [partition, setPartition] = useState(
    draft.partition === null ? ANY_PARTITION : String(draft.partition),
  );
  const [key, setKey] = useState(draft.key);
  const [value, setValue] = useState(draft.value);
  const [headers, setHeaders] = useState<HeaderRow[]>(() =>
    draft.headers.map((header, index) => ({ id: index + 1, ...header })),
  );

  const produce = useMutation({
    mutationFn: (record: ProduceRecord) =>
      postAndRead<ProducedRecord>(
        clusterPathname(cluster, "topics", encodeURIComponent(topic.name), "records"),
        record,
      ),
    // Not returned, so the write settles without waiting for loaded pages to refetch.
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: keys.topicRecords(cluster, topic.name) });
    },
  });

  const partitions = [
    { value: ANY_PARTITION, label: "Any" },
    ...topic.partitions.map((entry) => ({ value: String(entry.id), label: String(entry.id) })),
  ];

  const keyPayload = payload(key);
  const valuePayload = payload(value);
  const incomplete = keyPayload === undefined || valuePayload === undefined;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (produce.isPending || incomplete) return;
    produce.mutate(
      {
        partition: partition === ANY_PARTITION ? undefined : Number(partition),
        key: keyPayload,
        value: valuePayload,
        headers: headers
          .filter((header) => header.key !== "" || header.value !== "")
          .map((header) => ({ key: header.key, value: header.value })),
      },
      {
        // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
        onSuccess: onProduced,
      },
    );
  }

  function editHeader(rowId: number, patch: Partial<HeaderRow>) {
    setHeaders((rows) => rows.map((row) => (row.id === rowId ? { ...row, ...patch } : row)));
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Produce record</DialogTitle>
        <DialogDescription>
          Writes one record to <span className="font-mono">{topic.name}</span>.
        </DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-partition`}>Partition</Label>
        <Select
          items={partitions}
          value={partition}
          onValueChange={(next) => {
            if (next !== null) setPartition(next);
          }}
        >
          <SelectTrigger id={`${id}-partition`} className="w-32">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {partitions.map((entry) => (
              <SelectItem key={entry.value} value={entry.value}>
                {entry.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <PayloadField
        id={`${id}-key`}
        label="Key"
        cluster={cluster}
        subject={`${topic.name}-key`}
        subjects={subjects?.rows ?? []}
        draft={key}
        onChange={setKey}
      />
      <PayloadField
        id={`${id}-value`}
        label="Value"
        cluster={cluster}
        subject={`${topic.name}-value`}
        subjects={subjects?.rows ?? []}
        draft={value}
        onChange={setValue}
      />

      <div className="grid gap-1.5">
        <Label>Headers</Label>
        {headers.map((row) => (
          <div key={row.id} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] gap-2">
            <Input
              aria-label="Header name"
              autoComplete="off"
              spellCheck={false}
              className="font-mono"
              value={row.key}
              onChange={(event) => editHeader(row.id, { key: event.target.value })}
            />
            <Input
              aria-label="Header value"
              autoComplete="off"
              spellCheck={false}
              className="font-mono"
              value={row.value}
              onChange={(event) => editHeader(row.id, { value: event.target.value })}
            />
            <IconButton
              label="Remove header"
              size="icon"
              onClick={() => setHeaders((rows) => rows.filter((other) => other.id !== row.id))}
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
            setHeaders((rows) => [...rows, { id: (rows.at(-1)?.id ?? 0) + 1, key: "", value: "" }])
          }
        >
          <PlusIcon data-icon="inline-start" />
          Add header
        </Button>
      </div>

      {produce.isSuccess && !onProduced ? (
        <Alert>
          <CircleCheckIcon />
          <AlertDescription>
            Kafka stored the record in partition {produce.data.partition} at offset{" "}
            {produce.data.offset}.
          </AlertDescription>
        </Alert>
      ) : null}

      {produce.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription>
            {apiErrorMessage(produce.error, "Failed to produce the record.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={incomplete || produce.isPending}>
          {produce.isPending ? <Spinner data-icon="inline-start" /> : null}
          Produce
        </Button>
      </DialogFooter>
    </form>
  );
}

/** Undefined until a schema payload names its schema. */
function payload({ encoding, data, schemaId }: PayloadDraft): RecordPayload | null | undefined {
  if (encoding === "NULL") return null;
  if (encoding !== "SCHEMA") return { encoding, data };
  return schemaId === null ? undefined : { encoding, schemaId, data };
}

function PayloadField({
  id,
  label,
  cluster,
  subject,
  subjects,
  draft,
  onChange,
}: {
  id: string;
  label: string;
  cluster: string;
  subject: string;
  subjects: SubjectRow[];
  draft: PayloadDraft;
  onChange: (draft: PayloadDraft) => void;
}) {
  return (
    <div className="grid gap-1.5">
      <div className="flex items-center justify-between gap-2">
        <Label htmlFor={id}>{label}</Label>
        <ToggleGroup
          value={[draft.encoding]}
          onValueChange={(next) => {
            const encoding = next[0] as Encoding | undefined;
            if (!encoding) return;
            const schemaId =
              draft.schemaId ?? subjects.find((row) => row.subject === subject)?.id ?? null;
            onChange({ ...draft, encoding, schemaId });
          }}
          variant="outline"
          size="sm"
          spacing={0}
          className="[&_[data-slot=toggle-group-item]]:h-6 [&_[data-slot=toggle-group-item]]:px-2 [&_[data-slot=toggle-group-item]]:text-xs"
          aria-label={`${label} encoding`}
        >
          <ToggleGroupItem value="TEXT">Text</ToggleGroupItem>
          <ToggleGroupItem value="BASE64">Base64</ToggleGroupItem>
          {subjects.length > 0 ? <ToggleGroupItem value="SCHEMA">Schema</ToggleGroupItem> : null}
          <ToggleGroupItem value="NULL">Null</ToggleGroupItem>
        </ToggleGroup>
      </div>
      {draft.encoding === "SCHEMA" ? (
        <SchemaPicker
          cluster={cluster}
          preferred={subject}
          value={draft.schemaId}
          onChange={(schemaId) => onChange({ ...draft, schemaId })}
          label={`${label} schema`}
          placeholder="Choose a schema…"
        />
      ) : null}
      {draft.encoding === "NULL" ? null : (
        <Textarea
          id={id}
          spellCheck={false}
          className="max-h-48 font-mono"
          placeholder={draft.encoding === "SCHEMA" ? "JSON that fits the schema" : undefined}
          value={draft.data}
          onChange={(event) => onChange({ ...draft, data: event.target.value })}
        />
      )}
    </div>
  );
}
