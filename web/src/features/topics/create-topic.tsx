import { useId, useState, type FormEvent } from "react";
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
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
import { IconButton } from "@/components/icon-button";
import { apiErrorMessage, clusterPathname, post } from "@/lib/api/client";
import type { CreateTopic } from "@/lib/api/types";

const COUNT = /^[1-9]\d*$/;

type ConfigRow = { key: number; name: string; value: string };

export function CreateTopicDialog({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button />}>
        <PlusIcon data-icon="inline-start" />
        Create topic
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <CreateTopicForm cluster={cluster} onCreated={() => setOpen(false)} />
      </DialogContent>
    </Dialog>
  );
}

function CreateTopicForm({ cluster, onCreated }: { cluster: string; onCreated: () => void }) {
  const id = useId();
  const navigate = useNavigate();
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState("");
  const [replicationFactor, setReplicationFactor] = useState("");
  const [configs, setConfigs] = useState<ConfigRow[]>([]);

  const create = useMutation({
    mutationFn: (topic: CreateTopic) => post(clusterPathname(cluster, "topics"), topic),
  });

  const badPartitions = partitions !== "" && !COUNT.test(partitions);
  const badReplication = replicationFactor !== "" && !COUNT.test(replicationFactor);
  const ready = name.trim() !== "" && !badPartitions && !badReplication && !create.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    const topic = name.trim();
    create.mutate(
      {
        name: topic,
        partitions: partitions === "" ? undefined : Number(partitions),
        replicationFactor: replicationFactor === "" ? undefined : Number(replicationFactor),
        configs: Object.fromEntries(
          configs
            .filter((row) => row.name.trim() !== "")
            .map((row) => [row.name.trim(), row.value]),
        ),
      },
      {
        // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
        onSuccess: () => {
          onCreated();
          void navigate({ to: "/cluster/$cluster/topics/$topic", params: { cluster, topic } });
        },
      },
    );
  }

  function editConfig(key: number, patch: Partial<ConfigRow>) {
    setConfigs((rows) => rows.map((row) => (row.key === key ? { ...row, ...patch } : row)));
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Create topic</DialogTitle>
        <DialogDescription>Leave a count empty to use the broker default.</DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-name`}>Name</Label>
        <Input
          id={`${id}-name`}
          autoFocus
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          value={name}
          onChange={(event) => setName(event.target.value)}
        />
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-partitions`}>Partitions</Label>
          <Input
            id={`${id}-partitions`}
            inputMode="numeric"
            autoComplete="off"
            placeholder="Default"
            className="numeric"
            value={partitions}
            aria-invalid={badPartitions || undefined}
            onChange={(event) => setPartitions(event.target.value.trim())}
          />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-replication`}>Replication factor</Label>
          <Input
            id={`${id}-replication`}
            inputMode="numeric"
            autoComplete="off"
            placeholder="Default"
            className="numeric"
            value={replicationFactor}
            aria-invalid={badReplication || undefined}
            onChange={(event) => setReplicationFactor(event.target.value.trim())}
          />
        </div>
      </div>

      <div className="grid gap-1.5">
        <Label>Configs</Label>
        {configs.map((row) => (
          <div key={row.key} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] gap-2">
            <Input
              aria-label="Config name"
              autoComplete="off"
              spellCheck={false}
              placeholder="retention.ms"
              className="font-mono"
              value={row.name}
              onChange={(event) => editConfig(row.key, { name: event.target.value })}
            />
            <Input
              aria-label="Config value"
              autoComplete="off"
              spellCheck={false}
              className="font-mono"
              value={row.value}
              onChange={(event) => editConfig(row.key, { value: event.target.value })}
            />
            <IconButton
              label="Remove config"
              size="icon"
              onClick={() => setConfigs((rows) => rows.filter((other) => other.key !== row.key))}
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
            setConfigs((rows) => [
              ...rows,
              { key: (rows.at(-1)?.key ?? 0) + 1, name: "", value: "" },
            ])
          }
        >
          <PlusIcon data-icon="inline-start" />
          Add config
        </Button>
      </div>

      {create.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription>
            {apiErrorMessage(create.error, "Failed to create the topic.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={!ready}>
          {create.isPending ? <Spinner data-icon="inline-start" /> : null}
          Create topic
        </Button>
      </DialogFooter>
    </form>
  );
}
