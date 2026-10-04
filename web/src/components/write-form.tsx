import type { ComponentProps, FormEvent, ReactNode } from "react";
import { CircleAlertIcon } from "lucide-react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  DialogClose,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import {
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Spinner } from "@/components/ui/spinner";
import { Field } from "@/components/field";
import { cn } from "@/lib/utils";

export type Submit = {
  label: ReactNode;
  pending: boolean;
  disabled?: boolean;
  destructive?: boolean;
};

type WriteFormProps = {
  title: ReactNode;
  description?: ReactNode;
  error?: ReactNode;
  notice?: ReactNode;
  submit: Submit;
  secondary?: ReactNode;
  onSubmit: () => void;
  children?: ReactNode;
};

function handle(onSubmit: () => void, submit: Submit) {
  return (event: FormEvent) => {
    event.preventDefault();
    if (submit.pending || submit.disabled) return;
    onSubmit();
  };
}

function SubmitButton({ submit }: { submit: Submit }) {
  return (
    <Button
      type="submit"
      variant={submit.destructive ? "destructive" : "default"}
      disabled={submit.disabled || submit.pending}
    >
      {submit.pending ? <Spinner data-icon="inline-start" /> : null}
      {submit.label}
    </Button>
  );
}

function FormError({ children }: { children: ReactNode }) {
  return (
    <Alert variant="destructive">
      <CircleAlertIcon />
      <AlertDescription className="break-words">{children}</AlertDescription>
    </Alert>
  );
}

export function FormSheetContent({
  wide = false,
  className,
  ...props
}: ComponentProps<typeof SheetContent> & { wide?: boolean }) {
  return (
    <SheetContent
      side="right"
      className={cn(
        "w-full gap-0 data-[side=right]:w-full",
        wide ? "data-[side=right]:sm:max-w-2xl" : "data-[side=right]:sm:max-w-lg",
        className,
      )}
      {...props}
    />
  );
}

export function SheetForm({
  title,
  description,
  error,
  notice,
  submit,
  secondary,
  onSubmit,
  children,
}: WriteFormProps) {
  return (
    <form className="flex min-h-0 flex-1 flex-col" onSubmit={handle(onSubmit, submit)}>
      <SheetHeader className="gap-1 border-b px-5 py-4 pr-12">
        <SheetTitle className="truncate text-sm font-medium">{title}</SheetTitle>
        {description ? <SheetDescription>{description}</SheetDescription> : null}
      </SheetHeader>
      <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-5 py-4">{children}</div>
      <SheetFooter className="shrink-0 gap-3 border-t px-5 py-3">
        {notice}
        {error ? <FormError>{error}</FormError> : null}
        <div className="flex items-center justify-end gap-2">
          {secondary ? <div className="mr-auto flex items-center gap-2">{secondary}</div> : null}
          <SheetClose render={<Button type="button" variant="outline" />}>Cancel</SheetClose>
          <SubmitButton submit={submit} />
        </div>
      </SheetFooter>
    </form>
  );
}

export function DialogForm({
  title,
  description,
  error,
  notice,
  submit,
  secondary,
  onSubmit,
  children,
}: WriteFormProps) {
  return (
    <form className="grid min-w-0 gap-4" onSubmit={handle(onSubmit, submit)}>
      <DialogHeader>
        <DialogTitle>{title}</DialogTitle>
        {description ? <DialogDescription>{description}</DialogDescription> : null}
      </DialogHeader>
      {children}
      {notice}
      {error ? <FormError>{error}</FormError> : null}
      <DialogFooter>
        {secondary ? <div className="flex gap-2 sm:mr-auto">{secondary}</div> : null}
        <DialogClose render={<Button type="button" variant="outline" />}>Cancel</DialogClose>
        <SubmitButton submit={submit} />
      </DialogFooter>
    </form>
  );
}

export function TypeToConfirm({
  id,
  name,
  value,
  onChange,
}: {
  id: string;
  name: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <Field
      htmlFor={id}
      label={
        <span>
          Type <span className="font-mono text-foreground">{name}</span> to confirm
        </span>
      }
    >
      <Input
        id={id}
        autoFocus
        autoComplete="off"
        spellCheck={false}
        className="font-mono"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </Field>
  );
}
