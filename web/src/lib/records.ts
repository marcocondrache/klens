import type { KafkaRecord } from "@/lib/api/types";

/** A record's place in its topic, unique across partitions. */
export function recordId(record: KafkaRecord): string {
  return `${record.partition}-${record.offset}`;
}
