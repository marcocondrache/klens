import { useId, useState, type FormEvent } from "react";
import { LocateFixedIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Field, FieldError, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import type { RecordAddress } from "@/lib/api/live";
import type { TopicDetail } from "@/lib/api/types";

const DIGITS = /^\d+$/;

export function RecordJump({
  topic,
  onOpen,
}: {
  topic: TopicDetail;
  onOpen: (address: RecordAddress) => void;
}) {
  const id = useId();
  const [open, setOpen] = useState(false);
  const [partition, setPartition] = useState(
    topic.partitions.length === 1 ? String(topic.partitions[0].id) : "",
  );
  const [offset, setOffset] = useState("");

  const partitionId = DIGITS.test(partition) ? Number(partition) : null;
  const knownPartition =
    partitionId != null && topic.partitions.some((candidate) => candidate.id === partitionId);
  const badPartition = partition !== "" && !knownPartition;
  const offsetValue = DIGITS.test(offset) ? Number(offset) : null;
  const badOffset = offset !== "" && offsetValue == null;
  const ready = knownPartition && offsetValue != null;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (partitionId == null || !knownPartition || offsetValue == null) return;
    onOpen({ partition: partitionId, offset: offsetValue });
    setOpen(false);
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger render={<Button variant="outline" className="font-normal" />}>
        <LocateFixedIcon data-icon="inline-start" className="text-muted-foreground" />
        Go to offset
      </PopoverTrigger>
      <PopoverContent align="end" className="w-64 p-3">
        <form className="grid gap-3" onSubmit={submit}>
          <FieldGroup className="grid grid-cols-[5rem_minmax(0,1fr)] gap-2">
            <Field data-invalid={badPartition || undefined} className="gap-1.5">
              <FieldLabel htmlFor={`${id}-partition`} className="text-sm leading-none">
                Partition
              </FieldLabel>
              <Input
                id={`${id}-partition`}
                inputMode="numeric"
                autoComplete="off"
                className="numeric"
                value={partition}
                aria-invalid={badPartition || undefined}
                aria-describedby={badPartition ? `${id}-partition-error` : undefined}
                onChange={(event) => setPartition(event.target.value.trim())}
              />
            </Field>
            <Field data-invalid={badOffset || undefined} className="gap-1.5">
              <FieldLabel htmlFor={`${id}-offset`} className="text-sm leading-none">
                Offset
              </FieldLabel>
              <Input
                id={`${id}-offset`}
                inputMode="numeric"
                autoComplete="off"
                className="numeric"
                value={offset}
                aria-invalid={badOffset || undefined}
                aria-describedby={badOffset ? `${id}-offset-error` : undefined}
                onChange={(event) => setOffset(event.target.value.trim())}
              />
            </Field>
          </FieldGroup>
          {badPartition ? (
            <FieldError id={`${id}-partition-error`} className="text-xs">
              This topic has partitions 0 to {topic.partitions.length - 1}.
            </FieldError>
          ) : null}
          {badOffset ? (
            <FieldError id={`${id}-offset-error`} className="text-xs">
              Enter a whole number.
            </FieldError>
          ) : null}
          <Button type="submit" size="sm" disabled={!ready}>
            Open record
          </Button>
        </form>
      </PopoverContent>
    </Popover>
  );
}
