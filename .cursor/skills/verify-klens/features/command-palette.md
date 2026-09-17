# Command palette

The command palette jumps to a topic, group, broker, schema, or section without using the sidebar.

## Sub-features

- `palette-button` opens from the header `Search` button.
- `palette-slash` opens from `/` when focus is not in an editable field.
- `palette-modk` opens from `Meta+K` or `Control+K`.
- `palette-search` queries the cluster and lists a matching topic.
- `palette-goto` navigates from the Go to group (`Topics`, `Consumer Groups`, `Schema Registry`, `Brokers`, `ACLs`).

## How to get to it (user POV)

- Choose header button `Search` (visible from the `sm` breakpoint up).
- Press `/` outside a text field. On a page with `data-search-hotkey`, `/` focuses that field instead.
- Press `Meta+K` or `Control+K`.
- On a narrow viewport, choose the icon button named `Search`.

## Driving it with Playwright

Preconditions:

- Doctor reports `clusters` includes `local` and `catalogHealth` has `updatedAt` with no `lastError`.
- Topic `klens-verify-topics` exists.
- Viewport is at least 640px wide so the labeled `Search` button is shown.
- Start from `/cluster/local/topics`.

- **Button entry.** Click `Search`. Dialog `Search klens` appears. The input placeholder is `Search topics, groups, brokers and schemas…`. Go to lists `Topics`, `Consumer Groups`, `Schema Registry`, `Brokers`, and `ACLs`. Switch cluster lists `local`.
- **Keyboard entry.** Close the dialog and wait until it is hidden. Press `Control+K` (or dispatch `keydown` `k` with `ctrlKey` on `window` if Chrome steals the chord). The same dialog appears.
- **Topic match.** Type `klens-verify-topics`. A Topics group lists `klens-verify-topics` and that row is selected. ArrowDown does not move to Go to `Topics`. Choose the hit. The dialog closes and the URL is `/cluster/local/topics/klens-verify-topics`.
- **Go to.** Reopen the palette. Choose `ACLs` under Go to. The URL is `/cluster/local/acls`. Reopen and choose `Brokers`. The URL is `/cluster/local/nodes`.
- **Slash vs search field.** On Brokers (no `data-search-hotkey`), press `/`. The dialog opens. Close it. On Topics, press `/`. Focus moves to `Search topics…` and the dialog does not open.
- **Proof.** Screenshot the open dialog with the topic match visible. Save the URLs after the topic, ACLs, and Brokers navigations.

## Gotchas

- `/` focuses the first visible `data-search-hotkey` field instead of the palette. That includes Topics, Groups, Schemas, ACLs, the topic Data tab (`Search key or value…`), topic Configuration, and the broker node `Filter configuration…`. The Brokers list has no hotkey, so `/` opens the dialog there. See `findSearchHotkeyTarget` in `web/src/lib/keyboard.ts`.
- The header button visible label is `Search`. Its accessible name is `Search Ctrl+K` because the chord hint lives inside the button. Playwright `getByRole('button', { name: 'Search', exact: true })` misses it. Use `name: 'Search'` without `exact`, or `/^Search/`.
- Google Chrome on Linux may swallow `Control+K` (omnibox). Use the header `Search` button, or dispatch `keydown` on `window` with `key: "k"` and `ctrlKey: true`.
- Close the dialog and wait until it is hidden before the next open. `Control+K` toggles. A chord while the dialog is still closing closes it again.
- The dialog title is `Search klens` and is visually hidden (`sr-only`). Query it by accessible name, not by visible text.
- Results wait on GraphQL `search`. Wait for the topic row, not a fixed debounce sleep.
- A query with no hits shows `No matches in local.` Catalog hits replace the Go to and Switch cluster groups so arrow keys stay on the hits. Those groups return when the query misses the catalog but still matches a section or cluster name.
