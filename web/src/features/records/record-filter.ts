import { Rows3Icon } from "lucide-react";

import {
  selectedOptions,
  type FilterField,
  type FilterRule,
} from "@/components/data-table/filters";
import { formatCount } from "@/lib/format";
import type { KafkaRecord, TopicDetail } from "@/lib/api/types";

export type RecordFilter = {
  term: string;
  rules: FilterRule[];
  schemaId: number | null;
};

export const EMPTY_FILTER: RecordFilter = { term: "", rules: [], schemaId: null };

export function partitionField(topic: TopicDetail): FilterField<KafkaRecord> {
  return {
    id: "partition",
    label: "Partition",
    plural: "partitions",
    icon: Rows3Icon,
    options: topic.partitions.map((partition) => ({
      value: String(partition.id),
      label: String(partition.id),
      hint: formatCount(partition.retained),
    })),
    accessor: (record) => String(record.partition),
  };
}

export function filterPartitions(topic: TopicDetail, filter: RecordFilter) {
  const field = partitionField(topic);
  const rule = filter.rules.find((candidate) => candidate.id === field.id);
  if (!rule) return null;
  return selectedOptions(field, rule).map((option) => Number(option.value));
}
