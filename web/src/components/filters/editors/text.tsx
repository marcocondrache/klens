import { useState } from "react";

import { Input } from "@/components/ui/input";
import { EditorForm, type FilterEditorProps } from "@/components/filters/editors/shared";
import { cn } from "@/lib/utils";

export function TextEditor({
  field,
  value,
  header,
  error,
  onChange,
  onClose,
}: FilterEditorProps<"text">) {
  const [text, setText] = useState(value.text);

  return (
    <EditorForm
      header={header}
      error={error}
      onSubmit={() => {
        onChange({ kind: "text", text });
        onClose();
      }}
    >
      <Input
        autoFocus
        value={text}
        placeholder={field.placeholder ?? "Contains…"}
        aria-label={field.label}
        onChange={(event) => setText(event.target.value)}
        className={cn("h-8", field.mono && "font-mono")}
      />
    </EditorForm>
  );
}
