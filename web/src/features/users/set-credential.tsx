import { useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { PlusIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Dialog, type DialogHandle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Sheet, SheetTrigger } from "@/components/ui/sheet";
import { Field } from "@/components/field";
import { FormSheetContent, SheetForm } from "@/components/write-form";
import { apiErrorMessage, clusterPathname, put, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ScramMechanism, ScramUser, SetScramCredential } from "@/lib/api/types";

import { MechanismToggle } from "./mechanism-toggle";
import { MECHANISM_LABEL } from "./users-columns";

// Kafka refuses a credential outside these bounds.
const MIN_ITERATIONS = 4096;
const MAX_ITERATIONS = 16384;

type Change = { user: string; credential: SetScramCredential };

export function NewUserSheet({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger render={<Button variant="outline" className="ml-auto font-normal" />}>
        <PlusIcon className="text-muted-foreground" />
        Add user
      </SheetTrigger>
      <FormSheetContent>
        <CredentialForm cluster={cluster} onSaved={() => setOpen(false)} />
      </FormSheetContent>
    </Sheet>
  );
}

export function SetPasswordSheet({
  cluster,
  handle,
}: {
  cluster: string;
  handle: DialogHandle<ScramUser>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <FormSheetContent>
          {payload ? (
            <CredentialForm cluster={cluster} user={payload} onSaved={() => handle.close()} />
          ) : null}
        </FormSheetContent>
      )}
    </Dialog>
  );
}

function iterationsOf(user: ScramUser | undefined, mechanism: ScramMechanism) {
  const found = user?.credentials.find((credential) => credential.mechanism === mechanism);
  return String(found?.iterations ?? MIN_ITERATIONS);
}

function CredentialForm({
  cluster,
  user,
  onSaved,
}: {
  cluster: string;
  user?: ScramUser;
  onSaved: () => void;
}) {
  const id = useId();
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const [mechanism, setMechanism] = useState<ScramMechanism>(
    user?.credentials[0]?.mechanism ?? "SHA512",
  );
  const [password, setPassword] = useState("");
  const [iterations, setIterations] = useState(() => iterationsOf(user, mechanism));

  const save = useMutation({
    mutationFn: ({ user, credential }: Change) =>
      put(clusterPathname(cluster, "scram-users", resourceId(user)), credential),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: keys.scramUsers(cluster), exact: true }),
  });

  const target = user?.name ?? name.trim();
  const count = Number(iterations);
  const badIterations =
    !/^\d+$/.test(iterations) || count < MIN_ITERATIONS || count > MAX_ITERATIONS;
  const ready = target !== "" && password !== "" && !badIterations;
  const replaces = user?.credentials.some((credential) => credential.mechanism === mechanism);

  function submit() {
    // Unlike a hook-level onSuccess, this one is dropped once the sheet closes.
    save.mutate(
      { user: target, credential: { mechanism, password, iterations: count } },
      { onSuccess: onSaved },
    );
  }

  return (
    <SheetForm
      title={user ? "Set password" : "Add user"}
      description={
        replaces
          ? `Kafka replaces the ${MECHANISM_LABEL[mechanism]} password. Clients need the new one from their next login.`
          : "Kafka stores a SCRAM credential the user logs in with."
      }
      error={save.isError ? apiErrorMessage(save.error, "Failed to set the password.") : null}
      submit={{ label: "Save", pending: save.isPending, disabled: !ready }}
      onSubmit={submit}
    >
      <Field label="User" htmlFor={`${id}-user`}>
        <Input
          id={`${id}-user`}
          data-autofocus={!user || undefined}
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          disabled={user !== undefined}
          value={user?.name ?? name}
          onChange={(event) => setName(event.target.value)}
        />
      </Field>

      <Field label="Mechanism">
        <MechanismToggle
          value={mechanism}
          onChange={(next) => {
            setMechanism(next);
            setIterations(iterationsOf(user, next));
          }}
        />
      </Field>

      <Field label="Password" htmlFor={`${id}-password`}>
        <Input
          id={`${id}-password`}
          type="password"
          data-autofocus={user !== undefined || undefined}
          autoComplete="new-password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
        />
      </Field>

      <Field
        label="Iterations"
        htmlFor={`${id}-iterations`}
        hint={`Kafka takes ${MIN_ITERATIONS} to ${MAX_ITERATIONS} iterations.`}
      >
        <Input
          id={`${id}-iterations`}
          inputMode="numeric"
          autoComplete="off"
          className="numeric w-32"
          value={iterations}
          aria-invalid={badIterations || undefined}
          aria-describedby={`${id}-iterations-hint`}
          onChange={(event) => setIterations(event.target.value.trim())}
        />
      </Field>
    </SheetForm>
  );
}
