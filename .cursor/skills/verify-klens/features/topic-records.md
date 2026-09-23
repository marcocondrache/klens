# Topic records

Topic records shows the messages on one topic, newest first, and opens one message to read its key and value.

## Sub-features

- `records-list` shows the newest records on the Data tab.
- `records-search` keeps records whose key or value contains the query.
- `records-time` limits the scan to a from time and a to time.
- `records-partition` limits the scan to one partition, or all partitions.
- `records-order` switches between newest and oldest.
- `records-open` selects a row and shows that record's key and value.

## How to get to it (user POV)

- Open a topic from Topics. The Data tab is selected when the role can read records.
- Choose the **Data** tab on a topic that was opened on another tab.

## Driving it with Playwright

Preconditions:

- `helpers/doctor.sh` passed.
- The browser is on `/cluster/local/topics/klens-verify-topics`.
- The heading contains `klens-verify-topics`.

- **Data tab.** Click the tab named `Data` if it is not already selected. The placeholder `Search key or value…` is visible.
- **Seeded record.** A cell reads `verify-1` and a cell reads `hello-from-verify-klens`.
- **Search.** Fill `Search key or value…` with `hello-from-verify-klens`. The same value cell remains. Fill it with `no-such-record`. The empty title `No records` appears.
- **Partition.** The select shows `All partitions`. Choose `Partition 0`. The URL stays on the topic. The seeded record remains, because launch creates the topic with one partition.
- **Order.** The other select shows `Newest`. Choose `Oldest`. The seeded record remains.
- **Time fields.** The inputs named `From timestamp` and `To timestamp` are visible. Leave them empty for the seeded record.
- **Open payload.** Click the row that contains `hello-from-verify-klens`. The detail shows the topic name, partition, and offset.
- **Proof.** Save a screenshot and ARIA snapshot of the Data tab with the seeded value visible. Save `GET /api/clusters/local/topics/klens-verify-topics/records?order=NEWEST&limit=50`. The JSON `records` array includes `key` `verify-1` and `value` `hello-from-verify-klens`.

## Gotchas

- The Data tab is omitted when the role lacks `records`. Auth-off sessions have every privilege, so the tab is present.
- Record search is sent as the `contains` query on `GET /api/clusters/<cluster>/topics/<topic>/records`. It is not the Topics page search box.
- A partial scan shows the alert title `Partial scan`. The visible rows are real. Keep scrolling to continue. Do not treat that alert as an empty topic.
- Empty copy depends on the filters. A time range says `Nothing in the selected time range.` A text query says `Nothing matched your search in the scanned offsets.`
- The partition control is this select. It is not its own page.
- `obfuscated: true` in the records JSON badges the topic `Obfuscated`. The verify config has no obfuscation rules, so the seeded value is plain text.
