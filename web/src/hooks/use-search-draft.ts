import { useEffect, useRef, useState, type ChangeEvent } from "react";

const COMMIT_DELAY_MS = 150;

export function useSearchDraft(value: string, commit: (value: string) => void) {
  const [draft, setDraft] = useState(value);
  const [committed, setCommitted] = useState(value);
  const [previous, setPrevious] = useState(value);
  const timeout = useRef<ReturnType<typeof setTimeout>>(undefined);

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
