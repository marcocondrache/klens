/** True when the event target is already accepting typed text. */
export function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable) return true;

  const tag = target.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT";
}

/** First visible page search input marked with `data-search-hotkey`. */
export function findSearchHotkeyTarget(): HTMLInputElement | null {
  const candidates = document.querySelectorAll<HTMLInputElement>("[data-search-hotkey]");

  for (const input of candidates) {
    if (input.disabled) continue;
    if (input.getClientRects().length === 0) continue;
    return input;
  }

  return null;
}

/** Palette chord label. Matches the `metaKey || ctrlKey` listener in AppLayout. */
export function formatModK(): string {
  return appleHotkeyPlatform() ? "⌘K" : "Ctrl+K";
}

function appleHotkeyPlatform(): boolean {
  return typeof navigator !== "undefined" && /Mac|iPhone|iPod|iPad/.test(navigator.userAgent);
}
