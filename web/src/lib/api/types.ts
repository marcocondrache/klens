export type {
  Acl,
  AclPatternType,
  AclPermission,
  AclResourceType,
  Broker,
  CleanupPolicy,
  Cluster,
  ClusterStatus,
  Compression,
  ConfigEntry,
  ConfigSource,
  ConsumerGroup,
  ConsumerGroupMember,
  ConsumerGroupState,
  GroupOffset,
  MemberAssignment,
  Partition,
  RecordHeader,
  RecordOrder,
  RecordQuery,
  SchemaCompatibility,
  SchemaSubject,
  SchemaType,
  SearchResultKind,
  SecurityProtocol,
  ThroughputPoint,
  Topic,
  TopicRecord,
} from "./generated/graphql"

import type { SearchResult as SchemaSearchResult } from "./generated/graphql"

export type SearchResult = SchemaSearchResult & {
  href: string
}
