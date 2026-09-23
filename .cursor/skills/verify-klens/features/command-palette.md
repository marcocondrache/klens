# Command palette

The command palette jumps to a sidebar section, switches cluster, or opens a topic, group, broker, or schema match.

## Sub-features

- `palette-open` opens the dialog from the header or from `Control+K`.
- `palette-goto` navigates to a section from **Go to**.
- `palette-search` lists catalog hits for a non-empty query.
- `palette-empty` says there are no matches.
- `palette-switch` switches cluster from **Switch cluster**.

## How to get to it (user POV)

- Choose the header button labeled **Search**.
- Press `Control+K`, or `Command+K` on a Mac.
- Press `/` only when the page has no visible search box.

## Driving it with Playwright

Preconditions:

- `helpers/doctor.sh` passed.
- The browser is on `/cluster/local/topics` and the Topics heading is visible.

- **Open from the header.** Click the button named `Search`. The dialog named `Search klens` is visible. The placeholder is `Search topics, groups, brokers and schemas…`. **Go to** includes `Topics`, `Consumer Groups`, `Schema Registry`, `Brokers`, and `ACLs`. **Switch cluster** includes `local`.
- **Go to.** Choose `Brokers` inside the dialog. The dialog closes. The URL is `/cluster/local/nodes`. The heading is `Brokers`.
- **Search hit.** Open the palette again. Fill the palette placeholder with `klens-verify-topics`. A `Topics` group contains that name. Choose it. The URL is `/cluster/local/topics/klens-verify-topics`.
- **Empty.** Open the palette and fill `no-such-klens-hit`. The dialog says `No matches in local.`
- **Slash.** On the Topics page, press `/`. Focus moves to `Search topics…`. The dialog does not open.
- **Proof.** Save a screenshot and ARIA snapshot of the open dialog, and the URL after the topic hit. Corroborate with `GET /api/clusters/local/search?q=klens-verify-topics`.

## Gotchas

- The header button's accessible name includes the shortcut, so `Search` still matches it. The dialog's accessible name is `Search klens`, from the visually hidden title.
- `/` opens the palette only when no visible `data-search-hotkey` input exists. On Topics, Groups, Schemas, ACLs, and the Data tab, `/` focuses that page's search box.
- Palette search calls `GET /api/clusters/<cluster>/search?q=`. An empty query shows navigation instead of hits.
- Subject hits navigate to `/cluster/local/schemas?q=<subject>`. They do not open the schema sheet.
