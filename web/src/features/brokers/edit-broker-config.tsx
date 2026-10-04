import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Button } from "@/components/ui/button";
import { Dialog, type DialogHandle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Field } from "@/components/field";
import { FormSheetContent, SheetForm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, patch } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ConfigEntry, EditConfigs } from "@/lib/api/types";

type Scope = "BROKER" | "CLUSTER";

type Change = { scope: Scope; edit: EditConfigs };

export function EditBrokerConfigSheet({
  cluster,
  broker,
  handle,
}: {
  cluster: string;
  broker: number;
  handle: DialogHandle<ConfigEntry>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <FormSheetContent>
          {payload ? (
            <EditBrokerConfigForm
              cluster={cluster}
              broker={broker}
              entry={payload}
              onEdited={() => handle.close()}
            />
          ) : null}
        </FormSheetContent>
      )}
    </Dialog>
  );
}

function EditBrokerConfigForm({
  cluster,
  broker,
  entry,
  onEdited,
}: {
  cluster: string;
  broker: number;
  entry: ConfigEntry;
  onEdited: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const overridden = entry.source === "DYNAMIC_BROKER_CONFIG";
  const shared = entry.source === "DYNAMIC_DEFAULT_BROKER_CONFIG";
  const [value, setValue] = useState(entry.sensitive ? "" : (entry.value ?? ""));
  const [scope, setScope] = useState<Scope>(shared ? "CLUSTER" : "BROKER");

  const change = useMutation({
    mutationFn: ({ scope, edit }: Change) =>
      patch(
        scope === "BROKER"
          ? clusterPathname(cluster, "brokers", String(broker), "configs")
          : clusterPathname(cluster, "brokers", "configs"),
        edit,
      ),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: keys.brokerRows(cluster) }),
  });
  const resetting = change.isPending && change.variables.edit.reset.length > 0;
  const unchanged =
    !entry.sensitive && value === entry.value && (scope === "BROKER" ? overridden : shared);

  function save(next: Change) {
    // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
    change.mutate(next, { onSuccess: onEdited });
  }

  return (
    <SheetForm
      title={<span className="font-mono">{entry.name}</span>}
      description={
        overridden
          ? `Broker ${broker} sets its own value. Remove the override to follow the cluster again.`
          : shared
            ? "Every broker without a value of its own follows this cluster default."
            : "Kafka applies the new value without a restart."
      }
      error={change.isError ? apiErrorMessage(change.error, "Failed to change the config.") : null}
      submit={{
        label: "Save",
        pending: change.isPending && !resetting,
        disabled: change.isPending || unchanged,
      }}
      secondary={
        overridden || shared ? (
          <Button
            type="button"
            variant="outline"
            disabled={change.isPending}
            onClick={() =>
              save({
                scope: overridden ? "BROKER" : "CLUSTER",
                edit: { set: {}, reset: [entry.name] },
              })
            }
          >
            {resetting ? <Spinner data-icon="inline-start" /> : null}
            {overridden ? "Remove override" : "Remove default"}
          </Button>
        ) : null
      }
      onSubmit={() => save({ scope, edit: { set: { [entry.name]: value }, reset: [] } })}
    >
      <Field label="Value" htmlFor={`${id}-value`}>
        <Input
          id={`${id}-value`}
          data-autofocus
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          placeholder={entry.sensitive ? "Hidden" : undefined}
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
      </Field>

      <Field
        label="Applies to"
        hint={
          scope === "BROKER"
            ? `Broker ${broker} keeps this value whatever the cluster default.`
            : "Brokers that set their own value keep it."
        }
      >
        <ToggleGroup
          value={[scope]}
          onValueChange={(next) => {
            if (next[0]) setScope(next[0] === "CLUSTER" ? "CLUSTER" : "BROKER");
          }}
          variant="outline"
          size="sm"
          spacing={0}
          aria-label="Applies to"
        >
          <ToggleGroupItem value="BROKER">Broker {broker}</ToggleGroupItem>
          <ToggleGroupItem value="CLUSTER">Every broker</ToggleGroupItem>
        </ToggleGroup>
      </Field>
    </SheetForm>
  );
}
