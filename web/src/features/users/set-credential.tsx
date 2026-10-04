import { useId, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CircleAlertIcon, KeyRoundIcon, PlusIcon } from "lucide-react";

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
import { IconButton } from "@/components/icon-button";
import { apiErrorMessage, clusterPathname, put, resourceId } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { ScramMechanism, ScramUser, SetScramCredential } from "@/lib/api/types";

import { MechanismToggle } from "./mechanism-toggle";
import { MECHANISM_LABEL } from "./users-columns";

// Kafka refuses a credential outside these bounds.
const MIN_ITERATIONS = 4096;
const MAX_ITERATIONS = 16384;

type Change = { user: string; credential: SetScramCredential };

export function NewUserDialog({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button />}>
        <PlusIcon data-icon="inline-start" />
        Add user
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <CredentialForm cluster={cluster} onSaved={() => setOpen(false)} />
      </DialogContent>
    </Dialog>
  );
}

export function SetPasswordButton({
  handle,
  user,
}: {
  handle: DialogHandle<ScramUser>;
  user: ScramUser;
}) {
  return (
    <DialogTrigger
      handle={handle}
      payload={user}
      render={<IconButton label={`Set a password for ${user.name}`} tooltip="Set password" />}
    >
      <KeyRoundIcon />
    </DialogTrigger>
  );
}

export function SetPasswordDialog({
  cluster,
  handle,
}: {
  cluster: string;
  handle: DialogHandle<ScramUser>;
}) {
  return (
    <Dialog handle={handle}>
      {({ payload }) => (
        <DialogContent className="sm:max-w-md">
          {payload ? (
            <CredentialForm cluster={cluster} user={payload} onSaved={() => handle.close()} />
          ) : null}
        </DialogContent>
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
  const ready = target !== "" && password !== "" && !badIterations && !save.isPending;
  const replaces = user?.credentials.some((credential) => credential.mechanism === mechanism);

  function submit(event: FormEvent) {
    event.preventDefault();
    if (!ready) return;
    // Unlike a hook-level onSuccess, this one is dropped once the dialog closes.
    save.mutate(
      { user: target, credential: { mechanism, password, iterations: count } },
      { onSuccess: onSaved },
    );
  }

  return (
    <form className="grid gap-4" onSubmit={submit}>
      <DialogHeader>
        <DialogTitle>{user ? "Set password" : "Add user"}</DialogTitle>
        <DialogDescription>
          {replaces
            ? `Kafka replaces the ${MECHANISM_LABEL[mechanism]} password. Clients need the new one from their next login.`
            : "Kafka stores a SCRAM credential the user logs in with."}
        </DialogDescription>
      </DialogHeader>

      <div className="grid gap-1.5">
        <Label htmlFor={`${id}-user`}>User</Label>
        <Input
          id={`${id}-user`}
          autoFocus={!user}
          autoComplete="off"
          spellCheck={false}
          className="font-mono"
          disabled={user !== undefined}
          value={user?.name ?? name}
          onChange={(event) => setName(event.target.value)}
        />
      </div>

      <div className="grid gap-1.5">
        <Label>Mechanism</Label>
        <MechanismToggle
          value={mechanism}
          onChange={(next) => {
            setMechanism(next);
            setIterations(iterationsOf(user, next));
          }}
        />
      </div>

      <div className="grid grid-cols-[minmax(0,1fr)_10rem] gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-password`}>Password</Label>
          <Input
            id={`${id}-password`}
            type="password"
            autoFocus={user !== undefined}
            autoComplete="new-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
          />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor={`${id}-iterations`}>Iterations</Label>
          <Input
            id={`${id}-iterations`}
            inputMode="numeric"
            autoComplete="off"
            className="numeric"
            value={iterations}
            aria-invalid={badIterations || undefined}
            onChange={(event) => setIterations(event.target.value.trim())}
          />
        </div>
      </div>
      <p className="-mt-2 text-sm text-muted-foreground">
        Kafka takes {MIN_ITERATIONS} to {MAX_ITERATIONS} iterations.
      </p>

      {save.isError ? (
        <Alert variant="destructive">
          <CircleAlertIcon />
          <AlertDescription className="break-words">
            {apiErrorMessage(save.error, "Failed to set the password.")}
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
