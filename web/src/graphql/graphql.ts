/* eslint-disable */
/** Internal type. DO NOT USE DIRECTLY. */
type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
/** Internal type. DO NOT USE DIRECTLY. */
export type Incremental<T> =
  | T
  | { [P in keyof T]?: P extends " $fragmentName" | "__typename" ? T[P] : never };
import type { DocumentTypeDecoration } from "@graphql-typed-document-node/core";
export type CleanupPolicy = "COMPACT" | "COMPACT_DELETE" | "DELETE";

export type ClusterStatus = "DEGRADED" | "HEALTHY" | "OFFLINE";

export type Compression = "GZIP" | "LZ4" | "NONE" | "SNAPPY" | "ZSTD";

export type ConfigSource =
  | "DEFAULT_CONFIG"
  | "DYNAMIC_BROKER_CONFIG"
  | "DYNAMIC_TOPIC_CONFIG"
  | "STATIC_BROKER_CONFIG";

export type ConsumerGroupState =
  | "COMPLETING_REBALANCE"
  | "DEAD"
  | "EMPTY"
  | "PREPARING_REBALANCE"
  | "STABLE";

export type RecordOrder = "NEWEST" | "OLDEST";

export type RecordQuery = {
  cluster: string;
  cursor: string | null | undefined;
  filter: string | null | undefined;
  limit: number;
  order: RecordOrder;
  partition: number | null | undefined;
  schemaId: number | null | undefined;
  timestampFrom: string | null | undefined;
  timestampTo: string | null | undefined;
  topic: string;
};

export type SchemaCompatibility = "BACKWARD" | "FORWARD" | "FULL" | "NONE";

export type SchemaType = "AVRO" | "JSON" | "PROTOBUF";

export type SearchResultKind = "GROUP" | "NODE" | "SUBJECT" | "TOPIC";

export type SecurityProtocol = "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL" | "SSL";

export type ClusterFieldsFragment = {
  name: string;
  label: string;
  clusterId: string;
  bootstrapServers: Array<string>;
  securityProtocol: SecurityProtocol;
  version: string;
  status: ClusterStatus;
  brokerCount: number;
  topicCount: number;
  partitionCount: number;
  consumerGroupCount: number;
  underReplicatedPartitions: number;
  offlinePartitions: number;
  messageCount: number;
  sizeBytes: number;
  bytesInPerSec: number;
  bytesOutPerSec: number;
};

export type BrokerFieldsFragment = {
  id: number;
  host: string;
  port: number;
  rack: string | null;
  controller: boolean;
  partitionCount: number;
  leaderCount: number;
  logDirSizeBytes: number;
  bytesInPerSec: number;
  bytesOutPerSec: number;
};

export type PartitionFieldsFragment = {
  id: number;
  leader: number;
  replicas: Array<number>;
  isr: Array<number>;
  lowWatermark: number;
  highWatermark: number;
  sizeBytes: number;
};

export type TopicFieldsFragment = {
  name: string;
  internal: boolean;
  replicationFactor: number;
  messageCount: number;
  sizeBytes: number;
  cleanupPolicy: CleanupPolicy;
  retentionMs: number;
  consumerGroups: Array<string>;
  bytesInPerSec: number;
  messagesPerSec: number;
  underReplicated: boolean;
  partitions: Array<{
    id: number;
    leader: number;
    replicas: Array<number>;
    isr: Array<number>;
    lowWatermark: number;
    highWatermark: number;
    sizeBytes: number;
  }>;
};

export type ConfigEntryFieldsFragment = {
  name: string;
  value: string | null;
  source: ConfigSource;
  readOnly: boolean;
  sensitive: boolean;
  documentation: string | null;
};

export type MemberAssignmentFieldsFragment = { topic: string; partitions: Array<number> };

export type ConsumerGroupMemberFieldsFragment = {
  id: string;
  clientId: string;
  host: string;
  assignments: Array<{ topic: string; partitions: Array<number> }>;
};

export type GroupOffsetFieldsFragment = {
  topic: string;
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  memberId: string | null;
};

export type ConsumerGroupFieldsFragment = {
  id: string;
  state: ConsumerGroupState;
  protocol: string;
  coordinator: number;
  topics: Array<string>;
  lag: number;
  members: Array<{
    id: string;
    clientId: string;
    host: string;
    assignments: Array<{ topic: string; partitions: Array<number> }>;
  }>;
  offsets: Array<{
    topic: string;
    partition: number;
    currentOffset: number;
    endOffset: number;
    lag: number;
    memberId: string | null;
  }>;
};

export type ThroughputPointFieldsFragment = {
  timestamp: string;
  bytesIn: number;
  bytesOut: number;
  messages: number;
};

export type TopicRateFieldsFragment = {
  name: string;
  messagesPerSec: number;
  bytesInPerSec: number;
};

export type ConsumerGroupLagFieldsFragment = {
  id: string;
  lag: number;
  offsets: Array<{
    topic: string;
    partition: number;
    currentOffset: number;
    endOffset: number;
    lag: number;
    memberId: string | null;
  }>;
};

export type SchemaSubjectFieldsFragment = {
  subject: string;
  id: number;
  type: SchemaType;
  latestVersion: number;
  versions: Array<number>;
  compatibility: SchemaCompatibility;
  schema: string;
};

export type RecordHeaderFieldsFragment = { key: string; value: string };

export type TopicRecordFieldsFragment = {
  topic: string;
  partition: number;
  offset: number;
  timestamp: string;
  key: string | null;
  value: string | null;
  schemaId: number | null;
  sizeBytes: number;
  compression: Compression;
  headers: Array<{ key: string; value: string }>;
};

export type SearchResultFieldsFragment = {
  kind: SearchResultKind;
  id: string;
  label: string;
  detail: string;
};

export type ClustersQueryVariables = Exact<{ [key: string]: never }>;

export type ClustersQuery = {
  clusters: Array<{
    name: string;
    label: string;
    clusterId: string;
    bootstrapServers: Array<string>;
    securityProtocol: SecurityProtocol;
    version: string;
    status: ClusterStatus;
    brokerCount: number;
    topicCount: number;
    partitionCount: number;
    consumerGroupCount: number;
    underReplicatedPartitions: number;
    offlinePartitions: number;
    messageCount: number;
    sizeBytes: number;
    bytesInPerSec: number;
    bytesOutPerSec: number;
  }>;
};

export type ClusterQueryVariables = Exact<{
  name: string;
}>;

export type ClusterQuery = {
  cluster: {
    name: string;
    label: string;
    clusterId: string;
    bootstrapServers: Array<string>;
    securityProtocol: SecurityProtocol;
    version: string;
    status: ClusterStatus;
    brokerCount: number;
    topicCount: number;
    partitionCount: number;
    consumerGroupCount: number;
    underReplicatedPartitions: number;
    offlinePartitions: number;
    messageCount: number;
    sizeBytes: number;
    bytesInPerSec: number;
    bytesOutPerSec: number;
  } | null;
};

export type BrokersQueryVariables = Exact<{
  cluster: string;
}>;

export type BrokersQuery = {
  brokers: Array<{
    id: number;
    host: string;
    port: number;
    rack: string | null;
    controller: boolean;
    partitionCount: number;
    leaderCount: number;
    logDirSizeBytes: number;
    bytesInPerSec: number;
    bytesOutPerSec: number;
  }>;
};

export type BrokerQueryVariables = Exact<{
  cluster: string;
  id: number;
}>;

export type BrokerQuery = {
  broker: {
    id: number;
    host: string;
    port: number;
    rack: string | null;
    controller: boolean;
    partitionCount: number;
    leaderCount: number;
    logDirSizeBytes: number;
    bytesInPerSec: number;
    bytesOutPerSec: number;
  } | null;
};

export type BrokerConfigsQueryVariables = Exact<{
  cluster: string;
  id: number;
}>;

export type BrokerConfigsQuery = {
  brokerConfigs: Array<{
    name: string;
    value: string | null;
    source: ConfigSource;
    readOnly: boolean;
    sensitive: boolean;
    documentation: string | null;
  }>;
};

export type TopicsQueryVariables = Exact<{
  cluster: string;
}>;

export type TopicsQuery = {
  clusterCatalog: {
    updatedAt: string;
    topics: Array<{
      name: string;
      internal: boolean;
      replicationFactor: number;
      messageCount: number;
      sizeBytes: number;
      cleanupPolicy: CleanupPolicy;
      retentionMs: number;
      consumerGroups: Array<string>;
      bytesInPerSec: number;
      messagesPerSec: number;
      underReplicated: boolean;
      partitions: Array<{
        id: number;
        leader: number;
        replicas: Array<number>;
        isr: Array<number>;
        lowWatermark: number;
        highWatermark: number;
        sizeBytes: number;
      }>;
    }>;
  };
};

export type TopicQueryVariables = Exact<{
  cluster: string;
  name: string;
}>;

export type TopicQuery = {
  topic: {
    name: string;
    internal: boolean;
    replicationFactor: number;
    messageCount: number;
    sizeBytes: number;
    cleanupPolicy: CleanupPolicy;
    retentionMs: number;
    consumerGroups: Array<string>;
    bytesInPerSec: number;
    messagesPerSec: number;
    underReplicated: boolean;
    partitions: Array<{
      id: number;
      leader: number;
      replicas: Array<number>;
      isr: Array<number>;
      lowWatermark: number;
      highWatermark: number;
      sizeBytes: number;
    }>;
  } | null;
};

export type TopicConfigsQueryVariables = Exact<{
  cluster: string;
  name: string;
}>;

export type TopicConfigsQuery = {
  topicConfigs: Array<{
    name: string;
    value: string | null;
    source: ConfigSource;
    readOnly: boolean;
    sensitive: boolean;
    documentation: string | null;
  }>;
};

export type ConsumerGroupsQueryVariables = Exact<{
  cluster: string;
  topic: string | null | undefined;
}>;

export type ConsumerGroupsQuery = {
  consumerGroups: Array<{
    id: string;
    state: ConsumerGroupState;
    protocol: string;
    coordinator: number;
    topics: Array<string>;
    lag: number;
    members: Array<{
      id: string;
      clientId: string;
      host: string;
      assignments: Array<{ topic: string; partitions: Array<number> }>;
    }>;
    offsets: Array<{
      topic: string;
      partition: number;
      currentOffset: number;
      endOffset: number;
      lag: number;
      memberId: string | null;
    }>;
  }>;
};

export type GroupsCatalogQueryVariables = Exact<{
  cluster: string;
}>;

export type GroupsCatalogQuery = {
  clusterCatalog: {
    updatedAt: string;
    consumerGroups: Array<{
      id: string;
      state: ConsumerGroupState;
      protocol: string;
      coordinator: number;
      topics: Array<string>;
      lag: number;
      members: Array<{
        id: string;
        clientId: string;
        host: string;
        assignments: Array<{ topic: string; partitions: Array<number> }>;
      }>;
      offsets: Array<{
        topic: string;
        partition: number;
        currentOffset: number;
        endOffset: number;
        lag: number;
        memberId: string | null;
      }>;
    }>;
  };
};

export type ConsumerGroupQueryVariables = Exact<{
  cluster: string;
  id: string;
}>;

export type ConsumerGroupQuery = {
  consumerGroup: {
    id: string;
    state: ConsumerGroupState;
    protocol: string;
    coordinator: number;
    topics: Array<string>;
    lag: number;
    members: Array<{
      id: string;
      clientId: string;
      host: string;
      assignments: Array<{ topic: string; partitions: Array<number> }>;
    }>;
    offsets: Array<{
      topic: string;
      partition: number;
      currentOffset: number;
      endOffset: number;
      lag: number;
      memberId: string | null;
    }>;
  } | null;
};

export type ClusterThroughputQueryVariables = Exact<{
  cluster: string;
}>;

export type ClusterThroughputQuery = {
  clusterThroughput: Array<{
    timestamp: string;
    bytesIn: number;
    bytesOut: number;
    messages: number;
  }>;
};

export type TopicThroughputQueryVariables = Exact<{
  cluster: string;
  topic: string;
}>;

export type TopicThroughputQuery = {
  topicThroughput: Array<{
    timestamp: string;
    bytesIn: number;
    bytesOut: number;
    messages: number;
  }>;
};

export type GroupLagHistoryQueryVariables = Exact<{
  cluster: string;
  id: string;
}>;

export type GroupLagHistoryQuery = {
  groupLagHistory: Array<{
    timestamp: string;
    bytesIn: number;
    bytesOut: number;
    messages: number;
  }>;
};

export type SchemaSubjectsQueryVariables = Exact<{
  cluster: string;
}>;

export type SchemaSubjectsQuery = {
  schemaSubjects: Array<{
    subject: string;
    id: number;
    type: SchemaType;
    latestVersion: number;
    versions: Array<number>;
    compatibility: SchemaCompatibility;
    schema: string;
  }>;
};

export type RecordsQueryVariables = Exact<{
  query: RecordQuery;
}>;

export type RecordsQuery = {
  records: {
    hasMore: boolean;
    nextCursor: string | null;
    records: Array<{
      topic: string;
      partition: number;
      offset: number;
      timestamp: string;
      key: string | null;
      value: string | null;
      schemaId: number | null;
      sizeBytes: number;
      compression: Compression;
      headers: Array<{ key: string; value: string }>;
    }>;
  };
};

export type SearchQueryVariables = Exact<{
  cluster: string;
  term: string;
}>;

export type SearchQuery = {
  search: Array<{ kind: SearchResultKind; id: string; label: string; detail: string }>;
};

export type TopicRatesSubscriptionVariables = Exact<{
  cluster: string;
}>;

export type TopicRatesSubscription = {
  topicRates: Array<{ name: string; messagesPerSec: number; bytesInPerSec: number }>;
};

export type ConsumerGroupLagSubscriptionVariables = Exact<{
  cluster: string;
  id: string;
}>;

export type ConsumerGroupLagSubscription = {
  consumerGroupLag: {
    id: string;
    lag: number;
    offsets: Array<{
      topic: string;
      partition: number;
      currentOffset: number;
      endOffset: number;
      lag: number;
      memberId: string | null;
    }>;
  };
};

export class TypedDocumentString<TResult, TVariables>
  extends String
  implements DocumentTypeDecoration<TResult, TVariables>
{
  __apiType?: NonNullable<DocumentTypeDecoration<TResult, TVariables>["__apiType"]>;
  private value: string;
  public __meta__?: Record<string, any> | undefined;

  constructor(value: string, __meta__?: Record<string, any> | undefined) {
    super(value);
    this.value = value;
    this.__meta__ = __meta__;
  }

  override toString(): string & DocumentTypeDecoration<TResult, TVariables> {
    return this.value;
  }
}
export const ClusterFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment ClusterFields on Cluster {
  name
  label
  clusterId
  bootstrapServers
  securityProtocol
  version
  status
  brokerCount
  topicCount
  partitionCount
  consumerGroupCount
  underReplicatedPartitions
  offlinePartitions
  messageCount
  sizeBytes
  bytesInPerSec
  bytesOutPerSec
}
    `,
  { fragmentName: "ClusterFields" },
) as unknown as TypedDocumentString<ClusterFieldsFragment, unknown>;
export const BrokerFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment BrokerFields on Broker {
  id
  host
  port
  rack
  controller
  partitionCount
  leaderCount
  logDirSizeBytes
  bytesInPerSec
  bytesOutPerSec
}
    `,
  { fragmentName: "BrokerFields" },
) as unknown as TypedDocumentString<BrokerFieldsFragment, unknown>;
export const PartitionFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment PartitionFields on Partition {
  id
  leader
  replicas
  isr
  lowWatermark
  highWatermark
  sizeBytes
}
    `,
  { fragmentName: "PartitionFields" },
) as unknown as TypedDocumentString<PartitionFieldsFragment, unknown>;
export const TopicFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment TopicFields on Topic {
  name
  internal
  partitions {
    ...PartitionFields
  }
  replicationFactor
  messageCount
  sizeBytes
  cleanupPolicy
  retentionMs
  consumerGroups
  bytesInPerSec
  messagesPerSec
  underReplicated
}
    fragment PartitionFields on Partition {
  id
  leader
  replicas
  isr
  lowWatermark
  highWatermark
  sizeBytes
}`,
  { fragmentName: "TopicFields" },
) as unknown as TypedDocumentString<TopicFieldsFragment, unknown>;
export const ConfigEntryFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment ConfigEntryFields on ConfigEntry {
  name
  value
  source
  readOnly
  sensitive
  documentation
}
    `,
  { fragmentName: "ConfigEntryFields" },
) as unknown as TypedDocumentString<ConfigEntryFieldsFragment, unknown>;
export const MemberAssignmentFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
    `,
  { fragmentName: "MemberAssignmentFields" },
) as unknown as TypedDocumentString<MemberAssignmentFieldsFragment, unknown>;
export const ConsumerGroupMemberFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment ConsumerGroupMemberFields on ConsumerGroupMember {
  id
  clientId
  host
  assignments {
    ...MemberAssignmentFields
  }
}
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}`,
  { fragmentName: "ConsumerGroupMemberFields" },
) as unknown as TypedDocumentString<ConsumerGroupMemberFieldsFragment, unknown>;
export const GroupOffsetFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}
    `,
  { fragmentName: "GroupOffsetFields" },
) as unknown as TypedDocumentString<GroupOffsetFieldsFragment, unknown>;
export const ConsumerGroupFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment ConsumerGroupFields on ConsumerGroup {
  id
  state
  protocol
  coordinator
  members {
    ...ConsumerGroupMemberFields
  }
  topics
  lag
  offsets {
    ...GroupOffsetFields
  }
}
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
fragment ConsumerGroupMemberFields on ConsumerGroupMember {
  id
  clientId
  host
  assignments {
    ...MemberAssignmentFields
  }
}
fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}`,
  { fragmentName: "ConsumerGroupFields" },
) as unknown as TypedDocumentString<ConsumerGroupFieldsFragment, unknown>;
export const ThroughputPointFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment ThroughputPointFields on ThroughputPoint {
  timestamp
  bytesIn
  bytesOut
  messages
}
    `,
  { fragmentName: "ThroughputPointFields" },
) as unknown as TypedDocumentString<ThroughputPointFieldsFragment, unknown>;
export const TopicRateFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment TopicRateFields on TopicRate {
  name
  messagesPerSec
  bytesInPerSec
}
    `,
  { fragmentName: "TopicRateFields" },
) as unknown as TypedDocumentString<TopicRateFieldsFragment, unknown>;
export const ConsumerGroupLagFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment ConsumerGroupLagFields on ConsumerGroup {
  id
  lag
  offsets {
    ...GroupOffsetFields
  }
}
    fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}`,
  { fragmentName: "ConsumerGroupLagFields" },
) as unknown as TypedDocumentString<ConsumerGroupLagFieldsFragment, unknown>;
export const SchemaSubjectFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment SchemaSubjectFields on SchemaSubject {
  subject
  id
  type
  latestVersion
  versions
  compatibility
  schema
}
    `,
  { fragmentName: "SchemaSubjectFields" },
) as unknown as TypedDocumentString<SchemaSubjectFieldsFragment, unknown>;
export const RecordHeaderFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment RecordHeaderFields on RecordHeader {
  key
  value
}
    `,
  { fragmentName: "RecordHeaderFields" },
) as unknown as TypedDocumentString<RecordHeaderFieldsFragment, unknown>;
export const TopicRecordFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment TopicRecordFields on TopicRecord {
  topic
  partition
  offset
  timestamp
  key
  value
  schemaId
  headers {
    ...RecordHeaderFields
  }
  sizeBytes
  compression
}
    fragment RecordHeaderFields on RecordHeader {
  key
  value
}`,
  { fragmentName: "TopicRecordFields" },
) as unknown as TypedDocumentString<TopicRecordFieldsFragment, unknown>;
export const SearchResultFieldsFragmentDoc = new TypedDocumentString(
  `
    fragment SearchResultFields on SearchResult {
  kind
  id
  label
  detail
}
    `,
  { fragmentName: "SearchResultFields" },
) as unknown as TypedDocumentString<SearchResultFieldsFragment, unknown>;
export const ClustersDocument = new TypedDocumentString(`
    query Clusters {
  clusters {
    ...ClusterFields
  }
}
    fragment ClusterFields on Cluster {
  name
  label
  clusterId
  bootstrapServers
  securityProtocol
  version
  status
  brokerCount
  topicCount
  partitionCount
  consumerGroupCount
  underReplicatedPartitions
  offlinePartitions
  messageCount
  sizeBytes
  bytesInPerSec
  bytesOutPerSec
}`) as unknown as TypedDocumentString<ClustersQuery, ClustersQueryVariables>;
export const ClusterDocument = new TypedDocumentString(`
    query Cluster($name: String!) {
  cluster(name: $name) {
    ...ClusterFields
  }
}
    fragment ClusterFields on Cluster {
  name
  label
  clusterId
  bootstrapServers
  securityProtocol
  version
  status
  brokerCount
  topicCount
  partitionCount
  consumerGroupCount
  underReplicatedPartitions
  offlinePartitions
  messageCount
  sizeBytes
  bytesInPerSec
  bytesOutPerSec
}`) as unknown as TypedDocumentString<ClusterQuery, ClusterQueryVariables>;
export const BrokersDocument = new TypedDocumentString(`
    query Brokers($cluster: String!) {
  brokers(cluster: $cluster) {
    ...BrokerFields
  }
}
    fragment BrokerFields on Broker {
  id
  host
  port
  rack
  controller
  partitionCount
  leaderCount
  logDirSizeBytes
  bytesInPerSec
  bytesOutPerSec
}`) as unknown as TypedDocumentString<BrokersQuery, BrokersQueryVariables>;
export const BrokerDocument = new TypedDocumentString(`
    query Broker($cluster: String!, $id: Int!) {
  broker(cluster: $cluster, id: $id) {
    ...BrokerFields
  }
}
    fragment BrokerFields on Broker {
  id
  host
  port
  rack
  controller
  partitionCount
  leaderCount
  logDirSizeBytes
  bytesInPerSec
  bytesOutPerSec
}`) as unknown as TypedDocumentString<BrokerQuery, BrokerQueryVariables>;
export const BrokerConfigsDocument = new TypedDocumentString(`
    query BrokerConfigs($cluster: String!, $id: Int!) {
  brokerConfigs(cluster: $cluster, id: $id) {
    ...ConfigEntryFields
  }
}
    fragment ConfigEntryFields on ConfigEntry {
  name
  value
  source
  readOnly
  sensitive
  documentation
}`) as unknown as TypedDocumentString<BrokerConfigsQuery, BrokerConfigsQueryVariables>;
export const TopicsDocument = new TypedDocumentString(`
    query Topics($cluster: String!) {
  clusterCatalog(cluster: $cluster) {
    updatedAt
    topics {
      ...TopicFields
    }
  }
}
    fragment PartitionFields on Partition {
  id
  leader
  replicas
  isr
  lowWatermark
  highWatermark
  sizeBytes
}
fragment TopicFields on Topic {
  name
  internal
  partitions {
    ...PartitionFields
  }
  replicationFactor
  messageCount
  sizeBytes
  cleanupPolicy
  retentionMs
  consumerGroups
  bytesInPerSec
  messagesPerSec
  underReplicated
}`) as unknown as TypedDocumentString<TopicsQuery, TopicsQueryVariables>;
export const TopicDocument = new TypedDocumentString(`
    query Topic($cluster: String!, $name: String!) {
  topic(cluster: $cluster, name: $name) {
    ...TopicFields
  }
}
    fragment PartitionFields on Partition {
  id
  leader
  replicas
  isr
  lowWatermark
  highWatermark
  sizeBytes
}
fragment TopicFields on Topic {
  name
  internal
  partitions {
    ...PartitionFields
  }
  replicationFactor
  messageCount
  sizeBytes
  cleanupPolicy
  retentionMs
  consumerGroups
  bytesInPerSec
  messagesPerSec
  underReplicated
}`) as unknown as TypedDocumentString<TopicQuery, TopicQueryVariables>;
export const TopicConfigsDocument = new TypedDocumentString(`
    query TopicConfigs($cluster: String!, $name: String!) {
  topicConfigs(cluster: $cluster, name: $name) {
    ...ConfigEntryFields
  }
}
    fragment ConfigEntryFields on ConfigEntry {
  name
  value
  source
  readOnly
  sensitive
  documentation
}`) as unknown as TypedDocumentString<TopicConfigsQuery, TopicConfigsQueryVariables>;
export const ConsumerGroupsDocument = new TypedDocumentString(`
    query ConsumerGroups($cluster: String!, $topic: String) {
  consumerGroups(cluster: $cluster, topic: $topic) {
    ...ConsumerGroupFields
  }
}
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
fragment ConsumerGroupMemberFields on ConsumerGroupMember {
  id
  clientId
  host
  assignments {
    ...MemberAssignmentFields
  }
}
fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}
fragment ConsumerGroupFields on ConsumerGroup {
  id
  state
  protocol
  coordinator
  members {
    ...ConsumerGroupMemberFields
  }
  topics
  lag
  offsets {
    ...GroupOffsetFields
  }
}`) as unknown as TypedDocumentString<ConsumerGroupsQuery, ConsumerGroupsQueryVariables>;
export const GroupsCatalogDocument = new TypedDocumentString(`
    query GroupsCatalog($cluster: String!) {
  clusterCatalog(cluster: $cluster) {
    updatedAt
    consumerGroups {
      ...ConsumerGroupFields
    }
  }
}
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
fragment ConsumerGroupMemberFields on ConsumerGroupMember {
  id
  clientId
  host
  assignments {
    ...MemberAssignmentFields
  }
}
fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}
fragment ConsumerGroupFields on ConsumerGroup {
  id
  state
  protocol
  coordinator
  members {
    ...ConsumerGroupMemberFields
  }
  topics
  lag
  offsets {
    ...GroupOffsetFields
  }
}`) as unknown as TypedDocumentString<GroupsCatalogQuery, GroupsCatalogQueryVariables>;
export const ConsumerGroupDocument = new TypedDocumentString(`
    query ConsumerGroup($cluster: String!, $id: String!) {
  consumerGroup(cluster: $cluster, id: $id) {
    ...ConsumerGroupFields
  }
}
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
fragment ConsumerGroupMemberFields on ConsumerGroupMember {
  id
  clientId
  host
  assignments {
    ...MemberAssignmentFields
  }
}
fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}
fragment ConsumerGroupFields on ConsumerGroup {
  id
  state
  protocol
  coordinator
  members {
    ...ConsumerGroupMemberFields
  }
  topics
  lag
  offsets {
    ...GroupOffsetFields
  }
}`) as unknown as TypedDocumentString<ConsumerGroupQuery, ConsumerGroupQueryVariables>;
export const ClusterThroughputDocument = new TypedDocumentString(`
    query ClusterThroughput($cluster: String!) {
  clusterThroughput(cluster: $cluster) {
    ...ThroughputPointFields
  }
}
    fragment ThroughputPointFields on ThroughputPoint {
  timestamp
  bytesIn
  bytesOut
  messages
}`) as unknown as TypedDocumentString<ClusterThroughputQuery, ClusterThroughputQueryVariables>;
export const TopicThroughputDocument = new TypedDocumentString(`
    query TopicThroughput($cluster: String!, $topic: String!) {
  topicThroughput(cluster: $cluster, topic: $topic) {
    ...ThroughputPointFields
  }
}
    fragment ThroughputPointFields on ThroughputPoint {
  timestamp
  bytesIn
  bytesOut
  messages
}`) as unknown as TypedDocumentString<TopicThroughputQuery, TopicThroughputQueryVariables>;
export const GroupLagHistoryDocument = new TypedDocumentString(`
    query GroupLagHistory($cluster: String!, $id: String!) {
  groupLagHistory(cluster: $cluster, id: $id) {
    ...ThroughputPointFields
  }
}
    fragment ThroughputPointFields on ThroughputPoint {
  timestamp
  bytesIn
  bytesOut
  messages
}`) as unknown as TypedDocumentString<GroupLagHistoryQuery, GroupLagHistoryQueryVariables>;
export const SchemaSubjectsDocument = new TypedDocumentString(`
    query SchemaSubjects($cluster: String!) {
  schemaSubjects(cluster: $cluster) {
    ...SchemaSubjectFields
  }
}
    fragment SchemaSubjectFields on SchemaSubject {
  subject
  id
  type
  latestVersion
  versions
  compatibility
  schema
}`) as unknown as TypedDocumentString<SchemaSubjectsQuery, SchemaSubjectsQueryVariables>;
export const RecordsDocument = new TypedDocumentString(`
    query Records($query: RecordQuery!) {
  records(query: $query) {
    records {
      ...TopicRecordFields
    }
    hasMore
    nextCursor
  }
}
    fragment RecordHeaderFields on RecordHeader {
  key
  value
}
fragment TopicRecordFields on TopicRecord {
  topic
  partition
  offset
  timestamp
  key
  value
  schemaId
  headers {
    ...RecordHeaderFields
  }
  sizeBytes
  compression
}`) as unknown as TypedDocumentString<RecordsQuery, RecordsQueryVariables>;
export const SearchDocument = new TypedDocumentString(`
    query Search($cluster: String!, $term: String!) {
  search(cluster: $cluster, term: $term) {
    ...SearchResultFields
  }
}
    fragment SearchResultFields on SearchResult {
  kind
  id
  label
  detail
}`) as unknown as TypedDocumentString<SearchQuery, SearchQueryVariables>;
export const TopicRatesDocument = new TypedDocumentString(`
    subscription TopicRates($cluster: String!) {
  topicRates(cluster: $cluster) {
    ...TopicRateFields
  }
}
    fragment TopicRateFields on TopicRate {
  name
  messagesPerSec
  bytesInPerSec
}`) as unknown as TypedDocumentString<TopicRatesSubscription, TopicRatesSubscriptionVariables>;
export const ConsumerGroupLagDocument = new TypedDocumentString(`
    subscription ConsumerGroupLag($cluster: String!, $id: String!) {
  consumerGroupLag(cluster: $cluster, id: $id) {
    ...ConsumerGroupLagFields
  }
}
    fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}
fragment ConsumerGroupLagFields on ConsumerGroup {
  id
  lag
  offsets {
    ...GroupOffsetFields
  }
}`) as unknown as TypedDocumentString<
  ConsumerGroupLagSubscription,
  ConsumerGroupLagSubscriptionVariables
>;
