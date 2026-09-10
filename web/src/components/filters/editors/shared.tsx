import type { ReactNode } from "react";

import { Button } from "@/components/ui/button";
import { CommandInput } from "@/components/ui/command";
import { Kbd } from "@/components/ui/kbd";
import type { FieldOf, FilterKind, ValueOf } from "@/lib/filters/types";

export type FilterEditorProps<TKind extends FilterKind> = {
  field: FieldOf<TKind>;
  value: ValueOf<TKind>;
  /** Field switcher rendered at the top of every editor. */
  header: ReactNode;
  error?: string;
  onChange: (value: ValueOf<TKind>) => void;
  onClose: () => void;
};

/** Header row for editors built on `Command`: field switcher plus search box. */
export function EditorSearchHeader({
  header,
  placeholder = "Filter to…",
}: {
  header: ReactNode;
  placeholder?: string;
}) {
  return (
    <div className="flex items-center gap-1 pl-1 *:data-[slot=command-input-wrapper]:min-w-0 *:data-[slot=command-input-wrapper]:flex-1">
      {header}
      <CommandInput placeholder={placeholder} />
    </div>
  );
}

/** Editor body for value entry: header, fields, then an Apply submit button. */
export function EditorForm({
  header,
  error,
  disabled,
  children,
  onSubmit,
}: {
  header: ReactNode;
  error?: string;
  disabled?: boolean;
  children: ReactNode;
  onSubmit: () => void;
}) {
  return (
    <form
      className="flex flex-col gap-2 p-2"
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit();
      }}
    >
      <div className="flex items-center">{header}</div>
      {children}
      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      <Button type="submit" size="sm" disabled={disabled}>
        Apply
        <Kbd>⏎</Kbd>
      </Button>
    </form>
  );
}
