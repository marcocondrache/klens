import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, PencilIcon, PlusIcon, Trash2Icon } from "lucide-react";

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
  type DialogHandle,
} from "@/components/ui/dialog";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import { Switch } from "@/components/ui/switch";
import { IconButton } from "@/components/icon-button";
import { apiErrorMessage, clusterPathname, put } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ClientQuota, QuotaEntity, QuotaEntityType } from "@/lib/api/types";
import { formatBytes } from "@/lib/format";

import { ENTITY_LABEL, EntityCell } from "./quota-entity";

const KINDS: { value: string; label: string; parts: QuotaEntityType[] }[] = [
  { value: "USER", label: "User", parts: ["USER"] },
  { value: "CLIENT_ID", label: "Client ID", parts: ["CLIENT_ID"] },
  { value: "USER_CLIENT_ID", label: "User and client ID", parts: ["USER", "CLIENT_ID"] },
  { value: "IP", label: "IP", parts: ["IP"] },
];

type ValueKey =
  | "producerByteRate"
  | "consumerByteRate"
  | "requestPercentage"
  | "controllerMutationRate"
  | "connectionCreationRate";

const VALUES: { key: ValueKey; label: string; unit: string; bytes?: boolean }[] = [
  { key: "producerByteRate", label: "Produce", unit: "bytes/s", bytes: true },
  { key: "consumerByteRate", label: "Consume", unit: "bytes/s", bytes: true },
  { key: "requestPercentage", label: "Request time", unit: "%" },
  { key: "controllerMutationRate", label: "Mutations", unit: "/s" },
  { key: "connectionCreationRate", label: "Connections", unit: "/s" },
];

// Kafka limits an IP only by its connection rate, and a user or client ID by
// everything else.
function appliesTo(key: ValueKey, parts: QuotaEntityType[]) {
  return (key === "connectionCreationRate") === parts.includes("IP");
}

const NUMBER = /^\d+(\.\d+)?$/;

type NamedPart = { name: string; isDefault: boolean };
type Values = Record<ValueKey, string>;

function emptyQuota(entity: QuotaEntity[]): ClientQuota {
  return {
    entity,
    producerByteRate: null,
    consumerByteRate: null,
    requestPercentage: null,
    controllerMutationRate: null,
    connectionCreationRate: null,
  };
}

function useSetQuota(cluster: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (quota: ClientQuota) => put(clusterPathname(cluster, "quotas"), quota),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: keys.quotas(cluster), exact: true }),
  });
}

export function NewQuotaDialog({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button />}>
        <PlusIcon data-icon="inline-start" />
        Set quota
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <QuotaForm cluster={cluster} onSaved={() => setOpen(false)} />
      </DialogContent>
    </Dialog>
  );
}

export function EditQuotaButton({
  handle,
  quota,
}: {
  handle: DialogHandle<ClientQuota>;
  quota: ClientQuota;
}) {
  return (
    <DialogTrigger
      handle={handle}
      payload={quota}
      render={<IconButton label="Edit quota" tooltip="Edit" />}
    >
      <PencilIcon />
    </DialogTrigger>
  );
}

export function EditQuotaDialog({
  cluster,
  handle,
}: {
  cluster: string;
  handle: DialogHandle<ClientQuota>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <DialogContent className="sm:max-w-md">
          {payload ? (
            <QuotaForm cluster={cluster} quota={payload} onSaved={() => handle.close()} />
          ) : null}
        </DialogContent>
      )}
    </Dialog>
  );
}

function QuotaForm({
  cluster,
  quota,
  onSaved,
}: {
  cluster: string;
  quota?: ClientQuota;
  onSaved: () => void;
}) {
  const id = useId();
  const save = useSetQuota(cluster);
  const [kind, setKind] = useState(KINDS[0].value);
  const [names, setNames] = useState<Record<QuotaEntityType, NamedPart>>({
    USER: { name: "", isDefault: false },
    CLIENT_ID: { name: "", isDefault: false },
    IP: { name: "", isDefault: false },
  });
  const [values, setValues] = useState<Values>(() => {
    const pick = (key: ValueKey) => (quota?.[key] == null ? "" : String(quota[key]));
    return {
      producerByteRate: pick("producerByteRate"),
      consumerByteRate: pick("consumerByteRate"),
      requestPercentage: pick("requestPercentage"),
      controllerMutationRate: pick("controllerMutationRate"),
      connectionCreationRate: pick("connectionCreationRate"),
    };
  });

  const parts =
    quota?.entity.map((part) => part.entityType) ??
    KINDS.find((entry) => entry.value === kind)?.parts ??
    [];
  const entity: QuotaEntity[] =
    quota?.entity ??
    parts.map((entityType) => ({
      entityType,
      name: names[entityType].isDefault ? null : names[entityType].name.trim(),
    }));
  const shown = VALUES.filter((entry) => appliesTo(entry.key, parts));
  const filled = shown.filter((entry) => values[entry.key] !== "");
  const ready =
    entity.every((part) => part.name !== "") &&
    filled.length > 0 &&
    filled.every((entry) => NUMBER.test(values[entry.key])) &&
    !save.isPending;

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    const next = emptyQuota(entity);
    for (const entry of filled) next[entry.key] = Number(values[entry.key]);
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    save.mutate(next, { onSuccess: onSaved });
  }

  function editName(entityType: QuotaEntityType, patch: Partial<NamedPart>) {
    setNames((current) => ({ ...current, [entityType]: { ...current[entityType], ...patch } }));
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>{quota ? "Edit quota" : "Set quota"}</DialogTitle>
        <DialogDescription>
          Kafka throttles the clients that match the entity. Leave a value empty for no limit.
        </DialogDescription>
      </DialogHeader>

      {quota ? (
        <EntityCell parts={quota.entity} />
      ) : (
        <>
          <div className="grid gap-1.5">
            <Label htmlFor={`${id}-kind`}>Entity</Label>
            <Select
              items={KINDS}
              value={kind}
              onValueChange={(next) => {
                if (next !== null) setKind(next);
              }}
            >
              <SelectTrigger id={`${id}-kind`} className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {KINDS.map((entry) => (
                  <SelectItem key={entry.value} value={entry.value}>
                    {entry.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {parts.map((entityType) => (
            <div key={entityType} className="grid gap-1.5">
              <div className="flex items-center justify-between gap-2">
                <Label htmlFor={`${id}-${entityType}`}>{ENTITY_LABEL[entityType]}</Label>
                <Label className="font-normal text-muted-foreground">
                  <Switch
                    size="sm"
                    checked={names[entityType].isDefault}
                    onCheckedChange={(isDefault) => editName(entityType, { isDefault })}
                  />
                  Default
                </Label>
              </div>
              <InputGroup>
                <InputGroupInput
                  id={`${id}-${entityType}`}
                  autoComplete="off"
                  spellCheck={false}
                  className="font-mono"
                  placeholder={
                    names[entityType].isDefault ? "All without a quota of their own" : ""
                  }
                  disabled={names[entityType].isDefault}
                  value={names[entityType].isDefault ? "" : names[entityType].name}
                  onChange={(event) => editName(entityType, { name: event.target.value })}
                />
              </InputGroup>
            </div>
          ))}
        </>
      )}

      <div className="grid grid-cols-2 gap-3">
        {shown.map((entry) => {
          const value = values[entry.key];
          const bad = value !== "" && !NUMBER.test(value);
          return (
            <div key={entry.key} className="grid gap-1.5">
              <Label htmlFor={`${id}-${entry.key}`}>{entry.label}</Label>
              <InputGroup>
                <InputGroupInput
                  id={`${id}-${entry.key}`}
                  inputMode="decimal"
                  autoComplete="off"
                  className="numeric"
                  value={value}
                  aria-invalid={bad || undefined}
                  onChange={(event) =>
                    setValues((current) => ({ ...current, [entry.key]: event.target.value.trim() }))
                  }
                />
                <InputGroupAddon align="inline-end">
                  {entry.bytes && value !== "" && !bad
                    ? `${formatBytes(Number(value))}/s`
                    : entry.unit}
                </InputGroupAddon>
              </InputGroup>
            </div>
          );
        })}
      </div>

      {save.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(save.error, "Failed to set the quota.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={!ready}>
          {save.isPending ? <Spinner data-icon="inline-start" /> : null}
          Save
        </Button>
      </DialogFooter>
    </form>
  );
}

export function RemoveQuotaButton({
  handle,
  quota,
}: {
  handle: DialogHandle<ClientQuota>;
  quota: ClientQuota;
}) {
  return (
    <DialogTrigger
      handle={handle}
      payload={quota}
      render={<IconButton label="Remove quota" tooltip="Remove" />}
    >
      <Trash2Icon />
    </DialogTrigger>
  );
}

export function RemoveQuotaDialog({
  cluster,
  handle,
}: {
  cluster: string;
  handle: DialogHandle<ClientQuota>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <DialogContent className="sm:max-w-md">
          {payload ? (
            <RemoveQuotaForm cluster={cluster} quota={payload} onRemoved={() => handle.close()} />
          ) : null}
        </DialogContent>
      )}
    </Dialog>
  );
}

function RemoveQuotaForm({
  cluster,
  quota,
  onRemoved,
}: {
  cluster: string;
  quota: ClientQuota;
  onRemoved: () => void;
}) {
  const remove = useSetQuota(cluster);

  function submit(event: FormEvent) {
    event.preventDefault();
    if (remove.isPending) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    remove.mutate(emptyQuota(quota.entity), { onSuccess: onRemoved });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Remove quota</DialogTitle>
        <DialogDescription>
          Kafka stops throttling the clients that match this entity by its own quota.
        </DialogDescription>
      </DialogHeader>

      <EntityCell parts={quota.entity} />

      {remove.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(remove.error, "Failed to remove the quota.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" variant="destructive" disabled={remove.isPending}>
          {remove.isPending ? <Spinner data-icon="inline-start" /> : null}
          Remove quota
        </Button>
      </DialogFooter>
    </form>
  );
}
