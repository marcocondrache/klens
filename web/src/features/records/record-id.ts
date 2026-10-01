import type { KafkaRecord } from "@/lib/api/types";

export function recordId(record: KafkaRecord): string {
  return `${record.partition}-${record.offset}`;
}
