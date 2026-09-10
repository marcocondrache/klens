import { useState } from "react";

import { Button } from "@/components/ui/button";
import { ButtonGroup } from "@/components/ui/button-group";
import { Input } from "@/components/ui/input";
import { EditorForm, type FilterEditorProps } from "@/components/filters/editors/shared";
import { NUMBER_OPS } from "@/lib/filters/state";

function parse(text: string) {
  const trimmed = text.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

export function NumberEditor({
  field,
  value,
  header,
  error,
  onChange,
  onClose,
}: FilterEditorProps<"number">) {
  const [op, setOp] = useState(value.op);
  const [text, setText] = useState(value.value?.toString() ?? "");
  const ops = field.ops ? NUMBER_OPS.filter((entry) => field.ops?.includes(entry.id)) : NUMBER_OPS;

  return (
    <EditorForm
      header={header}
      error={error}
      onSubmit={() => {
        onChange({ kind: "number", op, value: parse(text) });
        onClose();
      }}
    >
      <div className="flex items-center gap-2">
        <ButtonGroup>
          {ops.map((entry) => (
            <Button
              key={entry.id}
              type="button"
              size="sm"
              variant={entry.id === op ? "secondary" : "outline"}
              aria-label={entry.label}
              aria-pressed={entry.id === op}
              onClick={() => setOp(entry.id)}
            >
              {entry.symbol}
            </Button>
          ))}
        </ButtonGroup>
        <Input
          autoFocus
          inputMode="numeric"
          value={text}
          placeholder={field.placeholder ?? "Value"}
          aria-label={field.label}
          onChange={(event) => setText(event.target.value)}
          className="h-8 min-w-0 flex-1"
        />
      </div>
      {field.unit ? <p className="text-xs text-muted-foreground">In {field.unit}.</p> : null}
    </EditorForm>
  );
}

export function NumberRangeEditor({
  field,
  value,
  header,
  error,
  onChange,
  onClose,
}: FilterEditorProps<"number-range">) {
  const [from, setFrom] = useState(value.from?.toString() ?? "");
  const [to, setTo] = useState(value.to?.toString() ?? "");

  return (
    <EditorForm
      header={header}
      error={error}
      onSubmit={() => {
        onChange({ kind: "number-range", from: parse(from), to: parse(to) });
        onClose();
      }}
    >
      <div className="flex items-center gap-2">
        <Input
          autoFocus
          inputMode="numeric"
          value={from}
          placeholder={field.fromPlaceholder ?? "From"}
          aria-label={`${field.label} from`}
          onChange={(event) => setFrom(event.target.value)}
          className="h-8 min-w-0 flex-1"
        />
        <span className="text-sm text-muted-foreground">to</span>
        <Input
          inputMode="numeric"
          value={to}
          placeholder={field.toPlaceholder ?? "To"}
          aria-label={`${field.label} to`}
          onChange={(event) => setTo(event.target.value)}
          className="h-8 min-w-0 flex-1"
        />
      </div>
    </EditorForm>
  );
}
