import { useId, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { PlusIcon, XIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { Field, FieldCount } from "@/components/field";
import { IconButton } from "@/components/icon-button";
import { FormSheetContent, SheetForm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, post } from "@/lib/api/client";
import type { CreateTopic } from "@/lib/api/types";

const COUNT = /^[1-9]\d*$/;

type ConfigRow = { key: number; name: string; value: string };

export function CreateTopicSheet({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger render={<Button variant="outline" className="font-normal" />}>
        <PlusIcon className="text-muted-foreground" />
        Create topic
      </SheetTrigger>
      <FormSheetContent>
        <CreateTopicForm cluster={cluster} onCreated={() => setOpen(false)} />
      </FormSheetContent>
    </Sheet>
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

  function submit() {
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
        // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
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
    <SheetForm
      title="Create topic"
      description="Leave a count empty to use the broker default."
      error={create.isError ? apiErrorMessage(create.error, "Failed to create the topic.") : null}
      submit={{
        label: "Create topic",
        pending: create.isPending,
        disabled: name.trim() === "" || badPartitions || badReplication,
      }}
      onSubmit={submit}
    >
      <Field label="Name" htmlFor={`${id}-name`}>
        <Input
          id={`${id}-name`}
          data-autofocus
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          value={name}
          onChange={(event) => setName(event.target.value)}
        />
      </Field>

      <div className="grid grid-cols-2 gap-3">
        <Field label="Partitions" htmlFor={`${id}-partitions`}>
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
        </Field>
        <Field label="Replication factor" htmlFor={`${id}-replication`}>
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
        </Field>
      </div>

      <Field
        label={
          <>
            Configs
            <FieldCount value={configs.length} />
          </>
        }
        action={
          <Button
            type="button"
            variant="ghost"
            size="xs"
            className="text-muted-foreground"
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
        }
      >
        {configs.length === 0 ? (
          <p className="text-sm text-muted-foreground">The topic follows the broker defaults.</p>
        ) : null}
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
      </Field>
    </SheetForm>
  );
}
