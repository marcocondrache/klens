export type {
  BrokerFieldsFragment as Broker,
  CleanupPolicy,
  ClusterFieldsFragment as Cluster,
  ClusterStatus,
  Compression,
  ConfigEntryFieldsFragment as ConfigEntry,
  ConfigSource,
  ConsumerGroupFieldsFragment as ConsumerGroup,
  ConsumerGroupMemberFieldsFragment as ConsumerGroupMember,
  ConsumerGroupState,
  GroupOffsetFieldsFragment as GroupOffset,
  MemberAssignmentFieldsFragment as MemberAssignment,
  PartitionFieldsFragment as Partition,
  RecordHeaderFieldsFragment as RecordHeader,
  RecordOrder,
  RecordQuery,
  SchemaCompatibility,
  SchemaSubjectFieldsFragment as SchemaSubject,
  SchemaType,
  SearchResultKind,
  SecurityProtocol,
  ThroughputPointFieldsFragment as ThroughputPoint,
  TopicFieldsFragment as Topic,
  TopicRateFieldsFragment as TopicRate,
  TopicRecordFieldsFragment as TopicRecord,
} from "@/graphql/graphql";

import type { SearchResultFieldsFragment } from "@/graphql/graphql";

export type SearchResult = SearchResultFieldsFragment & {
  href: string;
};
