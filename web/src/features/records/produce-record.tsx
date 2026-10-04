import { useId, useState, type ReactElement, type ReactNode } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleCheckIcon, PlusIcon, XIcon } from "lucide-react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { Textarea } from "@/components/ui/textarea";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Field, FieldCount } from "@/components/field";
import { IconButton } from "@/components/icon-button";
import { FormSheetContent, SheetForm } from "@/components/write-form";
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
import { cn } from "@/lib/utils";

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
 * Without `onProduced` the sheet stays open and says where Kafka stored the record.
 * A null `trigger` hides the button but keeps an open sheet and its edits.
 */
export function ProduceRecordSheet({
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
    <Sheet open={open} onOpenChange={setOpen}>
      {trigger ? <SheetTrigger render={trigger}>{children}</SheetTrigger> : null}
      <FormSheetContent wide>
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
      </FormSheetContent>
    </Sheet>
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

  function submit() {
    if (keyPayload === undefined || valuePayload === undefined) return;
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
        // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
        onSuccess: onProduced,
      },
    );
  }

  function editHeader(rowId: number, patch: Partial<HeaderRow>) {
    setHeaders((rows) => rows.map((row) => (row.id === rowId ? { ...row, ...patch } : row)));
  }

  return (
    <SheetForm
      title="Produce record"
      description={
        <>
          Writes one record to <span className="font-mono">{topic.name}</span>.
        </>
      }
      notice={
        produce.isSuccess && !onProduced ? (
          <Alert>
            <CircleCheckIcon />
            <AlertDescription>
              Kafka stored the record in partition {produce.data.partition} at offset{" "}
              {produce.data.offset}.
            </AlertDescription>
          </Alert>
        ) : null
      }
      error={
        produce.isError ? apiErrorMessage(produce.error, "Failed to produce the record.") : null
      }
      submit={{
        label: "Produce",
        pending: produce.isPending,
        disabled: keyPayload === undefined || valuePayload === undefined,
      }}
      onSubmit={submit}
    >
      <Field label="Partition" htmlFor={`${id}-partition`}>
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
      </Field>

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
        fill
      />

      <Field
        label={
          <>
            Headers
            <FieldCount value={headers.length} />
          </>
        }
        action={
          <Button
            type="button"
            variant="ghost"
            size="xs"
            className="text-muted-foreground"
            onClick={() =>
              setHeaders((rows) => [
                ...rows,
                { id: (rows.at(-1)?.id ?? 0) + 1, key: "", value: "" },
              ])
            }
          >
            <PlusIcon data-icon="inline-start" />
            Add header
          </Button>
        }
        className="shrink-0"
      >
        {headers.length === 0 ? <p className="text-sm text-muted-foreground">No headers.</p> : null}
        {headers.map((row) => (
          <div key={row.id} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] gap-2">
            <Input
              aria-label="Header name"
              autoComplete="off"
              spellCheck={false}
              placeholder="Name"
              className="font-mono"
              value={row.key}
              onChange={(event) => editHeader(row.id, { key: event.target.value })}
            />
            <Input
              aria-label="Header value"
              autoComplete="off"
              spellCheck={false}
              placeholder="Value"
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
      </Field>
    </SheetForm>
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
  fill = false,
}: {
  id: string;
  label: string;
  cluster: string;
  subject: string;
  subjects: SubjectRow[];
  draft: PayloadDraft;
  onChange: (draft: PayloadDraft) => void;
  fill?: boolean;
}) {
  return (
    <Field
      label={label}
      htmlFor={id}
      className={cn(fill ? "flex min-h-48 flex-1 flex-col" : "shrink-0")}
      action={
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
      }
    >
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
      {draft.encoding === "NULL" ? (
        <p className="text-sm text-muted-foreground">The record has no {label.toLowerCase()}.</p>
      ) : (
        <Textarea
          id={id}
          spellCheck={false}
          className={cn(
            "bg-subtle px-3 py-2.5 font-mono leading-relaxed md:text-sm dark:bg-subtle",
            fill ? "min-h-0 flex-1 resize-none field-sizing-fixed" : "max-h-40",
          )}
          placeholder={draft.encoding === "SCHEMA" ? "JSON that fits the schema" : undefined}
          value={draft.data}
          onChange={(event) => onChange({ ...draft, data: event.target.value })}
        />
      )}
    </Field>
  );
}
