import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, PencilIcon } from "lucide-react";

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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Spinner } from "@/components/ui/spinner";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { IconButton } from "@/components/icon-button";
import { apiErrorMessage, clusterPathname, patch } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ConfigEntry, EditConfigs } from "@/lib/api/types";

type Scope = "BROKER" | "CLUSTER";

type Change = { scope: Scope; edit: EditConfigs };

export function EditBrokerConfigButton({
  handle,
  entry,
}: {
  handle: DialogHandle<ConfigEntry>;
  entry: ConfigEntry;
}) {
  return (
    <DialogTrigger
      handle={handle}
      payload={entry}
      render={<IconButton label={`Edit ${entry.name}`} tooltip="Edit" />}
    >
      <PencilIcon />
    </DialogTrigger>
  );
}

export function EditBrokerConfigDialog({
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
        <DialogContent className="sm:max-w-md">
          {payload ? (
            <EditBrokerConfigForm
              cluster={cluster}
              broker={broker}
              entry={payload}
              onEdited={() => handle.close()}
            />
          ) : null}
        </DialogContent>
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
    if (change.isPending) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    change.mutate(next, { onSuccess: onEdited });
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    if (unchanged) return;
    save({ scope, edit: { set: { [entry.name]: value }, reset: [] } });
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>Edit config</DialogTitle>
        <DialogDescription>
          {overridden
            ? `Broker ${broker} sets its own value. Remove the override to follow the cluster again.`
            : shared
              ? "Every broker without a value of its own follows this cluster default."
              : "Kafka applies the new value without a restart."}
        </DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-value`} className="font-mono">
          {entry.name}
        </Label>
        <Input
          id={`${id}-value`}
          autoFocus
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          placeholder={entry.sensitive ? "Hidden" : undefined}
          value={value}
          onChange={(event) => setValue(event.target.value)}
        />
      </div>

      <div className="grid gap-1.5">
        <Label>Applies to</Label>
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
        <p className="text-sm text-muted-foreground">
          {scope === "BROKER"
            ? `Broker ${broker} keeps this value whatever the cluster default.`
            : "Brokers that set their own value keep it."}
        </p>
      </div>

      {change.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(change.error, "Failed to change the config.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <DialogFooter>
        {overridden || shared ? (
          <Button
            type="button"
            variant="outline"
            className="sm:mr-auto"
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
        ) : null}
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <Button type="submit" disabled={unchanged || change.isPending}>
          {change.isPending && !resetting ? <Spinner data-icon="inline-start" /> : null}
          Save
        </Button>
      </DialogFooter>
    </form>
  );
}
