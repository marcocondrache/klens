# Command palette

The command palette jumps to a topic, group, broker, schema, or section without using the sidebar.

## Sub-features

- `palette-button` opens from the header `Search` button.
- `palette-slash` opens from `/` when focus is not in an editable field.
- `palette-modk` opens from `Meta+K` or `Control+K`.
- `palette-search` queries the cluster and lists a matching topic.
- `palette-goto` navigates from the Go to group (`Topics`, `Consumer Groups`, `Schema Registry`, `Brokers`).

## How to get to it (user POV)

- Choose header button `Search` (visible from the `sm` breakpoint up).
- Press `/` outside a text field. On a page with `data-search-hotkey`, `/` focuses that field instead.
- Press `Meta+K` or `Control+K`.
- On a narrow viewport, choose the icon button named `Search`.

## Driving it with Playwright

Preconditions:

- Doctor reports `local` `HEALTHY`.
- Topic `klens-verify-topics` exists.
- Viewport is at least 640px wide so the labeled `Search` button is shown.
- Start from `/cluster/local/topics`.

- **Button entry.** Click `Search`. Dialog `Search klens` appears. The input placeholder is `Search topics, groups, brokers and schemas…`.
- **Keyboard entry.** Close the dialog. Press `Control+K`. The same dialog appears.
- **Topic match.** Type `klens-verify-topics`. A Topics group lists `klens-verify-topics`. Choose it. The dialog closes and the URL is `/cluster/local/topics/klens-verify-topics`.
- **Go to.** Reopen the palette. Choose `Brokers` under Go to. The URL is `/cluster/local/nodes`.
- **Slash vs search field.** On Topics, press `/`. Focus moves to `Search topics…` and the dialog does not open. That is correct for this page.
- **Proof.** Screenshot the open dialog with the topic match visible. Save the URL after the topic navigation.

## Gotchas

- `/` on Topics, Groups, or Schemas focuses the page search field (`findSearchHotkeyTarget` in `web/src/lib/keyboard.ts`). Use `Control+K` or the header button when you need the palette on those pages.
- The dialog title is `Search klens` and is visually hidden (`sr-only`). Query it by accessible name, not by visible text.
- Results wait on GraphQL `search`. Wait for the topic row, not a fixed debounce sleep.
- A query with no hits shows `No matches in local.`
