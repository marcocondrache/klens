export type {
  BrokerFieldsFragment as Broker,
  CleanupPolicy,
  ClusterStatus,
  ConfigEntryFieldsFragment as ConfigEntry,
  ConsumerGroupFieldsFragment as ConsumerGroup,
  GroupListFieldsFragment as GroupList,
  ConsumerGroupMemberFieldsFragment as ConsumerGroupMember,
  ConsumerGroupState,
  GroupOffsetFieldsFragment as GroupOffset,
  PartitionFieldsFragment as Partition,
  RecordOrder,
  RecordQuery,
  SchemaSubjectFieldsFragment as SchemaSubject,
  ThroughputPointFieldsFragment as ThroughputPoint,
  TopicFieldsFragment as Topic,
  TopicListFieldsFragment as TopicList,
  TopicRateFieldsFragment as TopicRate,
  TopicRecordFieldsFragment as TopicRecord,
} from "@/graphql/graphql";

import type { SearchResultFieldsFragment } from "@/graphql/graphql";

export type SearchResult = SearchResultFieldsFragment & {
  href: string;
};
