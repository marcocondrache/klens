import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Button } from "@/components/ui/button";
import { Dialog, type DialogHandle } from "@/components/ui/dialog";
import { Field, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { FormSheetContent, SheetForm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, patch } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ConfigEntry, EditConfigs } from "@/lib/api/types";

export function EditTopicConfigSheet({
  cluster,
  topic,
  handle,
}: {
  cluster: string;
  topic: string;
  handle: DialogHandle<ConfigEntry>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <FormSheetContent>
          {payload ? (
            <EditTopicConfigForm
              cluster={cluster}
              topic={topic}
              entry={payload}
              onEdited={() => handle.close()}
            />
          ) : null}
        </FormSheetContent>
      )}
    </Dialog>
  );
}

function EditTopicConfigForm({
  cluster,
  topic,
  entry,
  onEdited,
}: {
  cluster: string;
  topic: string;
  entry: ConfigEntry;
  onEdited: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [value, setValue] = useState(entry.sensitive ? "" : (entry.value ?? ""));
  const overridden = entry.source === "DYNAMIC_TOPIC_CONFIG";

  const edit = useMutation({
    mutationFn: (change: EditConfigs) =>
      patch(clusterPathname(cluster, "topics", encodeURIComponent(topic), "configs"), change),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: keys.topicConfigs(cluster, topic) }),
  });
  const resetting = edit.isPending && edit.variables.reset.length > 0;

  function save(change: EditConfigs) {
    // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
    edit.mutate(change, { onSuccess: onEdited });
  }

  return (
    <SheetForm
      title={<span className="font-mono">{entry.name}</span>}
      description={
        overridden ? (
          <>
            <span className="font-mono">{topic}</span> overrides the broker&apos;s value. Remove the
            override to follow the broker again.
          </>
        ) : (
          <>
            Saving sets an override on <span className="font-mono">{topic}</span> in place of the
            broker&apos;s value.
          </>
        )
      }
      error={edit.isError ? apiErrorMessage(edit.error, "Failed to change the config.") : null}
      submit={{
        label: "Save",
        pending: edit.isPending && !resetting,
        disabled: edit.isPending || (overridden && !entry.sensitive && value === entry.value),
      }}
      secondary={
        overridden ? (
          <Button
            type="button"
            variant="outline"
            disabled={edit.isPending}
            onClick={() => save({ set: {}, reset: [entry.name] })}
          >
            {resetting ? <Spinner data-icon="inline-start" /> : null}
            Remove override
          </Button>
        ) : null
      }
      onSubmit={() => save({ set: { [entry.name]: value }, reset: [] })}
    >
      <Field>
        <FieldLabel htmlFor={`${id}-value`}>Value</FieldLabel>
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
    </SheetForm>
  );
}
