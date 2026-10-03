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
import { apiErrorMessage, clusterPathname, postAndRead } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { PayloadEncoding, ProduceRecord, ProducedRecord, TopicDetail } from "@/lib/api/types";

type Encoding = PayloadEncoding | "NULL";

type PayloadDraft = { encoding: Encoding; data: string };

type HeaderRow = { id: number; key: string; value: string };

const NO_KEY: PayloadDraft = { encoding: "NULL", data: "" };

const EMPTY_VALUE: PayloadDraft = { encoding: "TEXT", data: "" };

const ANY_PARTITION = "any";

/** Without `onProduced` the dialog stays open and says where Kafka stored the record. */
export function ProduceRecordDialog({
  cluster,
  topic,
  trigger,
  children,
  onProduced,
}: {
  cluster: string;
  topic: TopicDetail;
  trigger: ReactElement;
  children: ReactNode;
  onProduced?: (record: ProducedRecord) => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={trigger}>{children}</DialogTrigger>
      <DialogContent className="sm:max-w-lg">
        <ProduceRecordForm
          cluster={cluster}
          topic={topic}
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
  onProduced,
}: {
  cluster: string;
  topic: TopicDetail;
  onProduced?: (record: ProducedRecord) => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [partition, setPartition] = useState(ANY_PARTITION);
  const [key, setKey] = useState(NO_KEY);
  const [value, setValue] = useState(EMPTY_VALUE);
  const [headers, setHeaders] = useState<HeaderRow[]>([]);

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

  function submit(event: FormEvent) {
    event.preventDefault();
    if (produce.isPending) return;
    produce.mutate(
      {
        partition: partition === ANY_PARTITION ? undefined : Number(partition),
        key: payload(key),
        value: payload(value),
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

      <PayloadField id={`${id}-key`} label="Key" draft={key} onChange={setKey} />
      <PayloadField id={`${id}-value`} label="Value" draft={value} onChange={setValue} />

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
        <Button type="submit" disabled={produce.isPending}>
          {produce.isPending ? <Spinner data-icon="inline-start" /> : null}
          Produce
        </Button>
      </DialogFooter>
    </form>
  );
}

function payload(draft: PayloadDraft): ProduceRecord["key"] {
  return draft.encoding === "NULL" ? null : { encoding: draft.encoding, data: draft.data };
}

function PayloadField({
  id,
  label,
  draft,
  onChange,
}: {
  id: string;
  label: string;
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
            if (encoding) onChange({ ...draft, encoding });
          }}
          variant="outline"
          size="sm"
          spacing={0}
          className="[&_[data-slot=toggle-group-item]]:h-6 [&_[data-slot=toggle-group-item]]:px-2 [&_[data-slot=toggle-group-item]]:text-xs"
          aria-label={`${label} encoding`}
        >
          <ToggleGroupItem value="TEXT">Text</ToggleGroupItem>
          <ToggleGroupItem value="BASE64">Base64</ToggleGroupItem>
          <ToggleGroupItem value="NULL">Null</ToggleGroupItem>
        </ToggleGroup>
      </div>
      {draft.encoding === "NULL" ? null : (
        <Textarea
          id={id}
          spellCheck={false}
          className="max-h-48 font-mono"
          value={draft.data}
          onChange={(event) => onChange({ ...draft, data: event.target.value })}
        />
      )}
    </div>
  );
}
