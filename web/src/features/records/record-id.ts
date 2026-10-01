import type { RecordAddress } from "@/lib/api/live";

export function recordId(record: RecordAddress): string {
  return `${record.partition}-${record.offset}`;
}
