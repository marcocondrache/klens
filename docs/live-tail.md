# Live tail

`GET /api/clusters/{cluster}/topics/{topic}/records/tail` follows a topic from
its current end as a server-sent event stream. It takes the same `partition`,
`contains`, and `schemaId` parameters as a record page. It needs the `records`
privilege, and it applies obfuscation the same way a page does. The first frame
is `ready` and names each partition's start offset. After that, `records` frames
arrive oldest first.

A tail samples a busy topic rather than streaming all of it. Each frame carries
at most `KLENS_TAIL_BATCH_LIMIT` (100) of the newest records, and frames are at
least `KLENS_TAIL_INTERVAL_MS` (250) apart. A partition that falls too far
behind skips ahead. `skipped` counts what was passed over. Each tail holds its
own consumer, and `KLENS_MAX_LIVE_TAILS` (32) caps how many run at once. Past
that cap, a new tail gets `503 TOO_MANY_TAILS`.
