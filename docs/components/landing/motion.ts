import { useSyncExternalStore } from 'react';

const reducedMotionQuery = '(prefers-reduced-motion: reduce)';
const storageKey = 'klens-motion';

const listeners = new Set<() => void>();
let paused: boolean | null = null;

function readPaused(): boolean {
  try {
    return localStorage.getItem(storageKey) === 'paused';
  } catch {
    return false;
  }
}

function getPaused(): boolean {
  paused ??= readPaused();

  return paused;
}

function subscribe(onChange: () => void) {
  const query = window.matchMedia(reducedMotionQuery);
  query.addEventListener('change', onChange);
  listeners.add(onChange);

  return () => {
    query.removeEventListener('change', onChange);
    listeners.delete(onChange);
  };
}

export function setReaderPaused(next: boolean) {
  paused = next;

  try {
    localStorage.setItem(storageKey, next ? 'paused' : 'running');
  } catch {
    // Storage can be off, as in a private window. The choice then lasts until reload.
  }

  for (const listener of listeners) listener();
}

export function useReducedMotion(): boolean {
  return useSyncExternalStore(
    subscribe,
    () => window.matchMedia(reducedMotionQuery).matches,
    () => false,
  );
}

export function useReaderPaused(): boolean {
  return useSyncExternalStore(subscribe, getPaused, () => false);
}

export function useMotionAllowed(): boolean {
  const reducedMotion = useReducedMotion();
  const readerPaused = useReaderPaused();

  return !reducedMotion && !readerPaused;
}
