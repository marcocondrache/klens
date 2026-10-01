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
