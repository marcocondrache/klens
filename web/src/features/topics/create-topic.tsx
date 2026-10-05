import { useId, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { PlusIcon, XIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldError, FieldLabel, FieldTitle } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { FieldCount } from "@/components/field-count";
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
        <PlusIcon data-icon="inline-start" className="text-muted-foreground" />
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
      <Field>
        <FieldLabel htmlFor={`${id}-name`}>Name</FieldLabel>
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
        <Field data-invalid={badPartitions || undefined}>
          <FieldLabel htmlFor={`${id}-partitions`}>Partitions</FieldLabel>
          <Input
            id={`${id}-partitions`}
            inputMode="numeric"
            autoComplete="off"
            placeholder="Default"
            className="numeric"
            value={partitions}
            aria-invalid={badPartitions || undefined}
            aria-describedby={badPartitions ? `${id}-partitions-error` : undefined}
            onChange={(event) => setPartitions(event.target.value.trim())}
          />
          {badPartitions ? (
            <FieldError id={`${id}-partitions-error`}>Enter a whole number above zero.</FieldError>
          ) : null}
        </Field>
        <Field data-invalid={badReplication || undefined}>
          <FieldLabel htmlFor={`${id}-replication`}>Replication factor</FieldLabel>
          <Input
            id={`${id}-replication`}
            inputMode="numeric"
            autoComplete="off"
            placeholder="Default"
            className="numeric"
            value={replicationFactor}
            aria-invalid={badReplication || undefined}
            aria-describedby={badReplication ? `${id}-replication-error` : undefined}
            onChange={(event) => setReplicationFactor(event.target.value.trim())}
          />
          {badReplication ? (
            <FieldError id={`${id}-replication-error`}>Enter a whole number above zero.</FieldError>
          ) : null}
        </Field>
      </div>

      <Field aria-labelledby={`${id}-configs`}>
        <div className="flex items-center justify-between gap-2">
          <FieldTitle id={`${id}-configs`}>
            Configs
            <FieldCount value={configs.length} />
          </FieldTitle>
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
        </div>
        {configs.length === 0 ? (
          <FieldDescription>The topic follows the broker defaults.</FieldDescription>
        ) : null}
        {configs.map((row, index) => (
          <Field key={row.key} orientation="horizontal">
            <Input
              aria-label={`Config ${index + 1} name`}
              autoComplete="off"
              spellCheck={false}
              placeholder="retention.ms"
              className="font-mono"
              value={row.name}
              onChange={(event) => editConfig(row.key, { name: event.target.value })}
            />
            <Input
              aria-label={`Config ${index + 1} value`}
              autoComplete="off"
              spellCheck={false}
              className="font-mono"
              value={row.value}
              onChange={(event) => editConfig(row.key, { value: event.target.value })}
            />
            <IconButton
              label={`Remove config ${index + 1}`}
              tooltip="Remove config"
              size="icon"
              onClick={() => setConfigs((rows) => rows.filter((other) => other.key !== row.key))}
            >
              <XIcon />
            </IconButton>
          </Field>
        ))}
      </Field>
    </SheetForm>
  );
}
