import { useState } from "react";

import { Textarea } from "@/components/ui/textarea";
import { EditorForm, type FilterEditorProps } from "@/components/filters/editors/shared";

/**
 * Free-form expression editor. Unlike the other editors it stays open after
 * applying, so a rejected expression can be corrected next to its error.
 */
export function RawEditor({ field, value, header, error, onChange }: FilterEditorProps<"raw">) {
  const [expression, setExpression] = useState(value.expression);

  return (
    <EditorForm
      header={header}
      error={error}
      onSubmit={() => onChange({ kind: "raw", expression })}
    >
      <Textarea
        autoFocus
        value={expression}
        placeholder={field.placeholder ?? "expression"}
        aria-label={field.label}
        aria-invalid={error ? true : undefined}
        spellCheck={false}
        onChange={(event) => setExpression(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
            event.currentTarget.form?.requestSubmit();
          }
        }}
        className="min-h-20 font-mono text-sm"
      />
      {field.hint ? <p className="text-xs text-muted-foreground">{field.hint}</p> : null}
    </EditorForm>
  );
}
