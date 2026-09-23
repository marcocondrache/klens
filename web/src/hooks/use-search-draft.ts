import { useEffect, useRef, useState, type ChangeEvent } from "react";

/** How long typing must pause before the term is committed. */
const COMMIT_DELAY_MS = 150;

/**
 * Input props for a search term kept in the URL. The router commits search
 * changes asynchronously, so the input edits a local draft instead: bound
 * straight to `value`, the caret would jump to the end on every keystroke.
 */
export function useSearchDraft(value: string, commit: (value: string) => void) {
  const [draft, setDraft] = useState(value);
  const [committed, setCommitted] = useState(value);
  const [previous, setPrevious] = useState(value);
  const timeout = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Adopt changes made elsewhere (history, links), but not echoes of our own commits.
  if (value !== previous) {
    setPrevious(value);
    if (value !== committed) {
      setCommitted(value);
      setDraft(value);
    }
  }

  useEffect(() => () => clearTimeout(timeout.current), []);

  function onChange(event: ChangeEvent<HTMLInputElement>) {
    const next = event.target.value;
    setDraft(next);
    clearTimeout(timeout.current);
    timeout.current = setTimeout(() => {
      setCommitted(next);
      commit(next);
    }, COMMIT_DELAY_MS);
  }

  return { value: draft, onChange };
}
