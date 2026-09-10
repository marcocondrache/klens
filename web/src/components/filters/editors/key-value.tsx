import { useState } from "react";

import { Input } from "@/components/ui/input";
import { EditorForm, type FilterEditorProps } from "@/components/filters/editors/shared";

export function KeyValueEditor({
  field,
  value,
  header,
  error,
  onChange,
  onClose,
}: FilterEditorProps<"key-value">) {
  const [key, setKey] = useState(value.key);
  const [text, setText] = useState(value.value);

  return (
    <EditorForm
      header={header}
      error={error}
      disabled={key.trim() === ""}
      onSubmit={() => {
        onChange({ kind: "key-value", key, value: text });
        onClose();
      }}
    >
      <Input
        autoFocus
        value={key}
        placeholder={field.keyPlaceholder ?? "Key"}
        aria-label={`${field.label} key`}
        onChange={(event) => setKey(event.target.value)}
        className="h-8 font-mono"
      />
      <Input
        value={text}
        placeholder={field.valuePlaceholder ?? "Value contains… (optional)"}
        aria-label={`${field.label} value`}
        onChange={(event) => setText(event.target.value)}
        className="h-8 font-mono"
      />
    </EditorForm>
  );
}
