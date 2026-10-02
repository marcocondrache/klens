import { useMutation } from "@tanstack/react-query";
import { useState, type ReactNode } from "react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Spinner } from "@/components/ui/spinner";
import { apiErrorMessage } from "@/lib/api/client";

export type ConfirmChangeProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: ReactNode;
  action: string;
  destructive?: boolean;
  /** The user types this name before a change that cannot be undone. */
  confirmName?: string;
  run: () => Promise<unknown>;
  onDone?: () => void;
};

/** Stays open with the server's reason when the change fails. */
export function ConfirmChange({
  open,
  onOpenChange,
  title,
  description,
  action,
  destructive = false,
  confirmName,
  run,
  onDone,
}: ConfirmChangeProps) {
  const [typed, setTyped] = useState("");
  const change = useMutation({
    mutationFn: run,
    onSuccess: () => {
      close();
      onDone?.();
    },
  });
  const armed = confirmName == null || typed === confirmName;

  function close() {
    setTyped("");
    change.reset();
    onOpenChange(false);
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (next) onOpenChange(true);
        else if (!change.isPending) close();
      }}
    >
      <DialogContent showCloseButton={!change.isPending}>
        <form
          className="grid gap-4"
          onSubmit={(event) => {
            event.preventDefault();
            if (armed && !change.isPending) change.mutate();
          }}
        >
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>{description}</DialogDescription>
          </DialogHeader>
          {confirmName != null && (
            <div className="grid gap-1.5">
              <Label htmlFor="confirm-change-name">
                Type <span className="font-mono">{confirmName}</span> to confirm
              </Label>
              <Input
                id="confirm-change-name"
                autoComplete="off"
                spellCheck={false}
                value={typed}
                disabled={change.isPending}
                onChange={(event) => setTyped(event.target.value)}
              />
            </div>
          )}
          {change.isError && (
            <Alert variant="destructive">
              <AlertDescription>
                {apiErrorMessage(change.error, "The change failed")}
              </AlertDescription>
            </Alert>
          )}
          <DialogFooter>
            <Button type="button" variant="outline" disabled={change.isPending} onClick={close}>
              Cancel
            </Button>
            <Button
              type="submit"
              variant={destructive ? "destructive" : "default"}
              disabled={!armed || change.isPending}
            >
              {change.isPending && <Spinner />}
              {action}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
