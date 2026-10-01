import { useId, useState, type FormEvent } from "react";
import { LocateFixedIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
  const unknownPartition =
    partitionId != null && !topic.partitions.some((candidate) => candidate.id === partitionId);
  const offsetValue = DIGITS.test(offset) ? Number(offset) : null;
  const ready = partitionId != null && !unknownPartition && offsetValue != null;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (partitionId == null || unknownPartition || offsetValue == null) return;
    onOpen({ partition: partitionId, offset: offsetValue });
    setOpen(false);
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger render={<Button variant="outline" className="gap-1.5 font-normal" />}>
        <LocateFixedIcon className="text-muted-foreground" />
        Go to offset
      </PopoverTrigger>
      <PopoverContent align="end" className="w-64 p-3">
        <form className="grid gap-3" onSubmit={submit}>
          <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-2">
            <div className="grid gap-1.5">
              <Label htmlFor={`${id}-partition`} className="text-muted-foreground">
                Partition
              </Label>
              <Input
                id={`${id}-partition`}
                inputMode="numeric"
                autoComplete="off"
                className="numeric"
                value={partition}
                aria-invalid={unknownPartition || undefined}
                onChange={(event) => setPartition(event.target.value.trim())}
              />
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor={`${id}-offset`} className="text-muted-foreground">
                Offset
              </Label>
              <Input
                id={`${id}-offset`}
                inputMode="numeric"
                autoComplete="off"
                className="numeric"
                value={offset}
                onChange={(event) => setOffset(event.target.value.trim())}
              />
            </div>
          </div>
          {unknownPartition ? (
            <p className="text-xs text-destructive">
              This topic has partitions 0 to {topic.partitions.length - 1}.
            </p>
          ) : null}
          <Button type="submit" size="sm" disabled={!ready}>
            Open record
          </Button>
        </form>
      </PopoverContent>
    </Popover>
  );
}
