import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { PlusIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Dialog, type DialogHandle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { Switch } from "@/components/ui/switch";
import { Field } from "@/components/field";
import {
  DialogForm,
  FormDialogContent,
  FormSheetContent,
  SheetForm,
} from "@/components/write-form";
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

export function NewQuotaSheet({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger render={<Button variant="outline" className="ml-auto font-normal" />}>
        <PlusIcon className="text-muted-foreground" />
        Set quota
      </SheetTrigger>
      <FormSheetContent>
        <QuotaForm cluster={cluster} onSaved={() => setOpen(false)} />
      </FormSheetContent>
    </Sheet>
  );
}

export function EditQuotaSheet({
  cluster,
  handle,
}: {
  cluster: string;
  handle: DialogHandle<ClientQuota>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <FormSheetContent>
          {payload ? (
            <QuotaForm cluster={cluster} quota={payload} onSaved={() => handle.close()} />
          ) : null}
        </FormSheetContent>
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
    filled.every((entry) => NUMBER.test(values[entry.key]));

  function submit() {
    const next = emptyQuota(entity);
    for (const entry of filled) next[entry.key] = Number(values[entry.key]);
    // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
    save.mutate(next, { onSuccess: onSaved });
  }

  function editName(entityType: QuotaEntityType, patch: Partial<NamedPart>) {
    setNames((current) => ({ ...current, [entityType]: { ...current[entityType], ...patch } }));
  }

  return (
    <SheetForm
      title={quota ? "Edit quota" : "Set quota"}
      description="Kafka throttles the clients that match the entity. Leave a value empty for no limit."
      error={save.isError ? apiErrorMessage(save.error, "Failed to set the quota.") : null}
      submit={{ label: "Save", pending: save.isPending, disabled: !ready }}
      onSubmit={submit}
    >
      {quota ? (
        <Field label="Entity">
          <EntityCell parts={quota.entity} />
        </Field>
      ) : (
        <>
          <Field label="Entity" htmlFor={`${id}-kind`}>
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
          </Field>

          {parts.map((entityType) => (
            <Field
              key={entityType}
              label={ENTITY_LABEL[entityType]}
              htmlFor={`${id}-${entityType}`}
              action={
                <Label className="text-xs font-normal text-muted-foreground">
                  <Switch
                    size="sm"
                    checked={names[entityType].isDefault}
                    onCheckedChange={(isDefault) => editName(entityType, { isDefault })}
                  />
                  Default
                </Label>
              }
            >
              <Input
                id={`${id}-${entityType}`}
                autoComplete="off"
                spellCheck={false}
                className="font-mono"
                placeholder={names[entityType].isDefault ? "All without a quota of their own" : ""}
                disabled={names[entityType].isDefault}
                value={names[entityType].isDefault ? "" : names[entityType].name}
                onChange={(event) => editName(entityType, { name: event.target.value })}
              />
            </Field>
          ))}
        </>
      )}

      <div className="grid grid-cols-2 gap-3">
        {shown.map((entry) => {
          const value = values[entry.key];
          const bad = value !== "" && !NUMBER.test(value);
          return (
            <Field key={entry.key} label={entry.label} htmlFor={`${id}-${entry.key}`}>
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
            </Field>
          );
        })}
      </div>
    </SheetForm>
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
        <FormDialogContent>
          {payload ? (
            <RemoveQuotaForm cluster={cluster} quota={payload} onRemoved={() => handle.close()} />
          ) : null}
        </FormDialogContent>
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

  return (
    <DialogForm
      title="Remove quota"
      description="Kafka stops throttling the clients that match this entity by its own quota."
      error={remove.isError ? apiErrorMessage(remove.error, "Failed to remove the quota.") : null}
      submit={{ label: "Remove quota", pending: remove.isPending, destructive: true }}
      // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
      onSubmit={() => remove.mutate(emptyQuota(quota.entity), { onSuccess: onRemoved })}
    >
      <EntityCell parts={quota.entity} />
    </DialogForm>
  );
}
