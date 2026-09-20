/* eslint-disable */
/** Internal type. DO NOT USE DIRECTLY. */
type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
/** Internal type. DO NOT USE DIRECTLY. */
export type Incremental<T> = T | { [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never };
import type { DocumentTypeDecoration } from '@graphql-typed-document-node/core';
export type AclAuthorizer =
  | 'DISABLED'
  | 'ENABLED';

export type AclOperation =
  | 'ALL'
  | 'ALTER'
  | 'ALTER_CONFIGS'
  | 'CLUSTER_ACTION'
  | 'CREATE'
  | 'DELETE'
  | 'DESCRIBE'
  | 'DESCRIBE_CONFIGS'
  | 'IDEMPOTENT_WRITE'
  | 'READ'
  | 'WRITE';

export type AclPatternType =
  | 'LITERAL'
  | 'PREFIXED';

export type AclPermission =
  | 'ALLOW'
  | 'DENY';

export type AclResourceType =
  | 'CLUSTER'
  | 'DELEGATION_TOKEN'
  | 'GROUP'
  | 'TOPIC'
  | 'TRANSACTIONAL_ID';

export type CleanupPolicy =
  | 'COMPACT'
  | 'COMPACT_DELETE'
  | 'DELETE';

export type Compression =
  | 'GZIP'
  | 'LZ4'
  | 'NONE'
  | 'SNAPPY'
  | 'ZSTD';

export type ConfigSource =
  | 'DEFAULT_CONFIG'
  | 'DYNAMIC_BROKER_CONFIG'
  | 'DYNAMIC_TOPIC_CONFIG'
  | 'STATIC_BROKER_CONFIG';

export type GroupState =
  | 'COMPLETING_REBALANCE'
  | 'DEAD'
  | 'EMPTY'
  | 'PREPARING_REBALANCE'
  | 'STABLE';

export type PrivilegeName =
  | 'ACLS'
  | 'CONFIGS'
  | 'RECORDS'
  | 'SCHEMA_TEXT';

/** A substring match or a CEL expression, never both. */
export type RecordFilterInput = {
  cel: string | null | undefined;
  contains: string | null | undefined;
};

export type RecordOrder =
  | 'NEWEST'
  | 'OLDEST';

export type RecordQueryInput = {
  cursor: string | null | undefined;
  filter: RecordFilterInput | null | undefined;
  from: string | null | undefined;
  limit: number;
  /**
   * Nullable rather than defaulted because juniper renders an enum
   * default as a quoted string, which is not valid SDL.
   */
  order: RecordOrder | null | undefined;
  partition: number | null | undefined;
  schemaId: number | null | undefined;
  to: string | null | undefined;
  topic: string;
};

export type ResyncReason =
  /** The client fell behind the change bus and missed events. */
  | 'LAGGED';

export type SchemaCompatibility =
  | 'BACKWARD'
  | 'FORWARD'
  | 'FULL'
  | 'NONE';

export type SchemaType =
  | 'AVRO'
  | 'JSON'
  | 'PROTOBUF';

export type SearchKind =
  | 'GROUP'
  | 'NODE'
  | 'SUBJECT'
  | 'TOPIC';

export type UpdateScope = {
  group: string | null | undefined;
  topic: string | null | undefined;
};

export type IdentityFieldsFragment = { subject: string | null, clusters: Array<{ cluster: string, roles: Array<string>, privileges: Array<PrivilegeName> }> };

export type LaneHealthFieldsFragment = { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean };

export type ClusterHealthFieldsFragment = { cluster: string, ready: boolean, topicCount: number, partitionCount: number, groupCount: number, brokerCount: number, subjectCount: number, underReplicatedPartitions: number, offlinePartitions: number, topology: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, watermarks: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, offsets: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, configs: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, subjects: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean } };

export type TopicRowFieldsFragment = { name: string, internal: boolean, partitionCount: number, replicationFactor: number, retainedMessages: string, producedTotal: string, rate: number, retentionMs: string, cleanupPolicy: CleanupPolicy, groupCount: number, underReplicated: boolean };

export type PartitionRowFieldsFragment = { id: number, leader: number, replicas: Array<number>, isr: Array<number>, lowWatermark: string, highWatermark: string, retained: string, underReplicated: boolean };

export type TopicDetailFieldsFragment = { name: string, internal: boolean, replicationFactor: number, retainedMessages: string, producedTotal: string, groupCount: number, underReplicated: boolean, partitions: Array<{ id: number, leader: number, replicas: Array<number>, isr: Array<number>, lowWatermark: string, highWatermark: string, retained: string, underReplicated: boolean }> };

export type TopicGroupRowFieldsFragment = { id: string, state: GroupState, memberCount: number, lagOnTopic: string };

export type GroupRowFieldsFragment = { id: string, state: GroupState, memberCount: number, topicNames: Array<string>, totalLag: string, lagComplete: boolean, coordinatorId: number };

export type MemberAssignmentFieldsFragment = { topic: string, partitions: Array<number> };

export type GroupMemberFieldsFragment = { id: string, clientId: string, host: string, assignments: Array<{ topic: string, partitions: Array<number> }> };

export type GroupOffsetFieldsFragment = { topic: string, partition: number, currentOffset: string, endOffset: string, lag: string, memberId: string | null };

export type GroupDetailFieldsFragment = { id: string, state: GroupState, protocol: string, coordinatorId: number, totalLag: string, lagComplete: boolean, members: Array<{ id: string, clientId: string, host: string, assignments: Array<{ topic: string, partitions: Array<number> }> }>, offsets: Array<{ topic: string, partition: number, currentOffset: string, endOffset: string, lag: string, memberId: string | null }> };

export type BrokerRowFieldsFragment = { id: number, host: string, port: number, rack: string | null, controller: boolean, partitionCount: number, leaderCount: number };

export type ConfigEntryFieldsFragment = { name: string, value: string | null, source: ConfigSource, readOnly: boolean, sensitive: boolean };

export type SubjectRowFieldsFragment = { subject: string, id: number, type: SchemaType, latestVersion: number, versions: Array<number>, compatibility: SchemaCompatibility };

export type SubjectDetailFieldsFragment = { subject: string, version: number, id: number, type: SchemaType, schema: string, references: Array<{ name: string, subject: string, version: number }> };

export type AclFieldsFragment = { resourceType: AclResourceType, resourceName: string, patternType: AclPatternType, principal: string, host: string, operation: AclOperation, permission: AclPermission };

export type RecordHeaderFieldsFragment = { key: string, value: string };

export type RecordFieldsFragment = { topic: string, partition: number, offset: string, timestamp: string, key: string | null, value: string | null, schemaId: number | null, sizeBytes: string, compression: Compression, headers: Array<{ key: string, value: string }> };

export type SearchHitFieldsFragment = { kind: SearchKind, id: string, label: string, detail: string };

export type WhoamiQueryVariables = Exact<{ [key: string]: never; }>;


export type WhoamiQuery = { whoami: { subject: string | null, clusters: Array<{ cluster: string, roles: Array<string>, privileges: Array<PrivilegeName> }> } };

export type ClustersQueryVariables = Exact<{ [key: string]: never; }>;


export type ClustersQuery = { clusters: Array<{ name: string, health: { cluster: string, ready: boolean, topicCount: number, partitionCount: number, groupCount: number, brokerCount: number, subjectCount: number, underReplicatedPartitions: number, offlinePartitions: number, topology: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, watermarks: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, offsets: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, configs: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean }, subjects: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean } } }> };

export type TopicRowsQueryVariables = Exact<{
  cluster: string;
}>;


export type TopicRowsQuery = { cluster: { topics: { rows: Array<{ name: string, internal: boolean, partitionCount: number, replicationFactor: number, retainedMessages: string, producedTotal: string, rate: number, retentionMs: string, cleanupPolicy: CleanupPolicy, groupCount: number, underReplicated: boolean }> } } | null };

export type TopicQueryVariables = Exact<{
  cluster: string;
  name: string;
}>;


export type TopicQuery = { cluster: { topic: { name: string, internal: boolean, replicationFactor: number, retainedMessages: string, producedTotal: string, groupCount: number, underReplicated: boolean, partitions: Array<{ id: number, leader: number, replicas: Array<number>, isr: Array<number>, lowWatermark: string, highWatermark: string, retained: string, underReplicated: boolean }> } | null, topics: { rows: Array<{ name: string, internal: boolean, partitionCount: number, replicationFactor: number, retainedMessages: string, producedTotal: string, rate: number, retentionMs: string, cleanupPolicy: CleanupPolicy, groupCount: number, underReplicated: boolean }> } } | null };

export type TopicGroupsQueryVariables = Exact<{
  cluster: string;
  topic: string;
}>;


export type TopicGroupsQuery = { cluster: { topicGroups: Array<{ id: string, state: GroupState, memberCount: number, lagOnTopic: string }> } | null };

export type TopicConfigsQueryVariables = Exact<{
  cluster: string;
  name: string;
}>;


export type TopicConfigsQuery = { cluster: { topicConfigs: Array<{ name: string, value: string | null, source: ConfigSource, readOnly: boolean, sensitive: boolean }> } | null };

export type GroupRowsQueryVariables = Exact<{
  cluster: string;
}>;


export type GroupRowsQuery = { cluster: { groups: { rows: Array<{ id: string, state: GroupState, memberCount: number, topicNames: Array<string>, totalLag: string, lagComplete: boolean, coordinatorId: number }> } } | null };

export type GroupQueryVariables = Exact<{
  cluster: string;
  id: string;
}>;


export type GroupQuery = { cluster: { group: { id: string, state: GroupState, protocol: string, coordinatorId: number, totalLag: string, lagComplete: boolean, members: Array<{ id: string, clientId: string, host: string, assignments: Array<{ topic: string, partitions: Array<number> }> }>, offsets: Array<{ topic: string, partition: number, currentOffset: string, endOffset: string, lag: string, memberId: string | null }> } | null } | null };

export type BrokerRowsQueryVariables = Exact<{
  cluster: string;
}>;


export type BrokerRowsQuery = { cluster: { brokers: Array<{ id: number, host: string, port: number, rack: string | null, controller: boolean, partitionCount: number, leaderCount: number }> } | null };

export type BrokerConfigsQueryVariables = Exact<{
  cluster: string;
  id: number;
}>;


export type BrokerConfigsQuery = { cluster: { brokerConfigs: Array<{ name: string, value: string | null, source: ConfigSource, readOnly: boolean, sensitive: boolean }> } | null };

export type SubjectRowsQueryVariables = Exact<{
  cluster: string;
}>;


export type SubjectRowsQuery = { cluster: { subjects: { rows: Array<{ subject: string, id: number, type: SchemaType, latestVersion: number, versions: Array<number>, compatibility: SchemaCompatibility }>, sourceHealth: { updatedAt: string | null, checkedAt: string | null, lastError: string | null, lastPollMs: string | null, healthy: boolean } } } | null };

export type SubjectQueryVariables = Exact<{
  cluster: string;
  name: string;
  version: number | null | undefined;
}>;


export type SubjectQuery = { cluster: { subject: { subject: string, version: number, id: number, type: SchemaType, schema: string, references: Array<{ name: string, subject: string, version: number }> } } | null };

export type AclsQueryVariables = Exact<{
  cluster: string;
}>;


export type AclsQuery = { cluster: { acls: { authorizer: AclAuthorizer, bindings: Array<{ resourceType: AclResourceType, resourceName: string, patternType: AclPatternType, principal: string, host: string, operation: AclOperation, permission: AclPermission }> } } | null };

export type RecordsQueryVariables = Exact<{
  cluster: string;
  query: RecordQueryInput;
}>;


export type RecordsQuery = { cluster: { records: { complete: boolean, obfuscated: boolean, nextCursor: string | null, prevCursor: string | null, records: Array<{ topic: string, partition: number, offset: string, timestamp: string, key: string | null, value: string | null, schemaId: number | null, sizeBytes: string, compression: Compression, headers: Array<{ key: string, value: string }> }> } } | null };

export type SearchQueryVariables = Exact<{
  cluster: string;
  term: string;
}>;


export type SearchQuery = { cluster: { search: Array<{ kind: SearchKind, id: string, label: string, detail: string }> } | null };

export type UpdatesSubscriptionVariables = Exact<{
  cluster: string;
  scope: UpdateScope | null | undefined;
}>;


export type UpdatesSubscription = { updates:
    | { __typename: 'ConfigsChanged', version: string, configTopics: Array<string> }
    | { __typename: 'GroupLagUpdate', group: string, lag: string, lagComplete: boolean, offsets: Array<{ topic: string, partition: number, currentOffset: string, endOffset: string, lag: string, memberId: string | null }> }
    | { __typename: 'Resync', reason: ResyncReason }
    | { __typename: 'SubjectsChanged', version: string, added: Array<string>, removed: Array<string>, changed: Array<string> }
    | { __typename: 'TopologyDelta', version: string, addedTopics: Array<string>, removedTopics: Array<string>, changedTopics: Array<string>, addedGroups: Array<string>, removedGroups: Array<string>, changedGroups: Array<string>, brokersChanged: boolean }
    | { __typename: 'WatermarksTick', topics: Array<{ topic: string, rate: number }> }
   };

export class TypedDocumentString<TResult, TVariables>
  extends String
  implements DocumentTypeDecoration<TResult, TVariables>
{
  __apiType?: NonNullable<DocumentTypeDecoration<TResult, TVariables>['__apiType']>;
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
export const IdentityFieldsFragmentDoc = new TypedDocumentString(`
    fragment IdentityFields on Identity {
  subject
  clusters {
    cluster
    roles
    privileges
  }
}
    `, {"fragmentName":"IdentityFields"}) as unknown as TypedDocumentString<IdentityFieldsFragment, unknown>;
export const LaneHealthFieldsFragmentDoc = new TypedDocumentString(`
    fragment LaneHealthFields on LaneHealth {
  updatedAt
  checkedAt
  lastError
  lastPollMs
  healthy
}
    `, {"fragmentName":"LaneHealthFields"}) as unknown as TypedDocumentString<LaneHealthFieldsFragment, unknown>;
export const ClusterHealthFieldsFragmentDoc = new TypedDocumentString(`
    fragment ClusterHealthFields on ClusterHealth {
  cluster
  ready
  topology {
    ...LaneHealthFields
  }
  watermarks {
    ...LaneHealthFields
  }
  offsets {
    ...LaneHealthFields
  }
  configs {
    ...LaneHealthFields
  }
  subjects {
    ...LaneHealthFields
  }
  topicCount
  partitionCount
  groupCount
  brokerCount
  subjectCount
  underReplicatedPartitions
  offlinePartitions
}
    fragment LaneHealthFields on LaneHealth {
  updatedAt
  checkedAt
  lastError
  lastPollMs
  healthy
}`, {"fragmentName":"ClusterHealthFields"}) as unknown as TypedDocumentString<ClusterHealthFieldsFragment, unknown>;
export const TopicRowFieldsFragmentDoc = new TypedDocumentString(`
    fragment TopicRowFields on TopicRow {
  name
  internal
  partitionCount
  replicationFactor
  retainedMessages
  producedTotal
  rate
  retentionMs
  cleanupPolicy
  groupCount
  underReplicated
}
    `, {"fragmentName":"TopicRowFields"}) as unknown as TypedDocumentString<TopicRowFieldsFragment, unknown>;
export const PartitionRowFieldsFragmentDoc = new TypedDocumentString(`
    fragment PartitionRowFields on PartitionRow {
  id
  leader
  replicas
  isr
  lowWatermark
  highWatermark
  retained
  underReplicated
}
    `, {"fragmentName":"PartitionRowFields"}) as unknown as TypedDocumentString<PartitionRowFieldsFragment, unknown>;
export const TopicDetailFieldsFragmentDoc = new TypedDocumentString(`
    fragment TopicDetailFields on TopicDetail {
  name
  internal
  replicationFactor
  retainedMessages
  producedTotal
  groupCount
  underReplicated
  partitions {
    ...PartitionRowFields
  }
}
    fragment PartitionRowFields on PartitionRow {
  id
  leader
  replicas
  isr
  lowWatermark
  highWatermark
  retained
  underReplicated
}`, {"fragmentName":"TopicDetailFields"}) as unknown as TypedDocumentString<TopicDetailFieldsFragment, unknown>;
export const TopicGroupRowFieldsFragmentDoc = new TypedDocumentString(`
    fragment TopicGroupRowFields on TopicGroupRow {
  id
  state
  memberCount
  lagOnTopic
}
    `, {"fragmentName":"TopicGroupRowFields"}) as unknown as TypedDocumentString<TopicGroupRowFieldsFragment, unknown>;
export const GroupRowFieldsFragmentDoc = new TypedDocumentString(`
    fragment GroupRowFields on GroupRow {
  id
  state
  memberCount
  topicNames
  totalLag
  lagComplete
  coordinatorId
}
    `, {"fragmentName":"GroupRowFields"}) as unknown as TypedDocumentString<GroupRowFieldsFragment, unknown>;
export const MemberAssignmentFieldsFragmentDoc = new TypedDocumentString(`
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
    `, {"fragmentName":"MemberAssignmentFields"}) as unknown as TypedDocumentString<MemberAssignmentFieldsFragment, unknown>;
export const GroupMemberFieldsFragmentDoc = new TypedDocumentString(`
    fragment GroupMemberFields on GroupMember {
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
}`, {"fragmentName":"GroupMemberFields"}) as unknown as TypedDocumentString<GroupMemberFieldsFragment, unknown>;
export const GroupOffsetFieldsFragmentDoc = new TypedDocumentString(`
    fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}
    `, {"fragmentName":"GroupOffsetFields"}) as unknown as TypedDocumentString<GroupOffsetFieldsFragment, unknown>;
export const GroupDetailFieldsFragmentDoc = new TypedDocumentString(`
    fragment GroupDetailFields on GroupDetail {
  id
  state
  protocol
  coordinatorId
  totalLag
  lagComplete
  members {
    ...GroupMemberFields
  }
  offsets {
    ...GroupOffsetFields
  }
}
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
fragment GroupMemberFields on GroupMember {
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
}`, {"fragmentName":"GroupDetailFields"}) as unknown as TypedDocumentString<GroupDetailFieldsFragment, unknown>;
export const BrokerRowFieldsFragmentDoc = new TypedDocumentString(`
    fragment BrokerRowFields on BrokerRow {
  id
  host
  port
  rack
  controller
  partitionCount
  leaderCount
}
    `, {"fragmentName":"BrokerRowFields"}) as unknown as TypedDocumentString<BrokerRowFieldsFragment, unknown>;
export const ConfigEntryFieldsFragmentDoc = new TypedDocumentString(`
    fragment ConfigEntryFields on ConfigEntry {
  name
  value
  source
  readOnly
  sensitive
}
    `, {"fragmentName":"ConfigEntryFields"}) as unknown as TypedDocumentString<ConfigEntryFieldsFragment, unknown>;
export const SubjectRowFieldsFragmentDoc = new TypedDocumentString(`
    fragment SubjectRowFields on SubjectRow {
  subject
  id
  type
  latestVersion
  versions
  compatibility
}
    `, {"fragmentName":"SubjectRowFields"}) as unknown as TypedDocumentString<SubjectRowFieldsFragment, unknown>;
export const SubjectDetailFieldsFragmentDoc = new TypedDocumentString(`
    fragment SubjectDetailFields on SubjectDetail {
  subject
  version
  id
  type
  schema
  references {
    name
    subject
    version
  }
}
    `, {"fragmentName":"SubjectDetailFields"}) as unknown as TypedDocumentString<SubjectDetailFieldsFragment, unknown>;
export const AclFieldsFragmentDoc = new TypedDocumentString(`
    fragment AclFields on Acl {
  resourceType
  resourceName
  patternType
  principal
  host
  operation
  permission
}
    `, {"fragmentName":"AclFields"}) as unknown as TypedDocumentString<AclFieldsFragment, unknown>;
export const RecordHeaderFieldsFragmentDoc = new TypedDocumentString(`
    fragment RecordHeaderFields on RecordHeader {
  key
  value
}
    `, {"fragmentName":"RecordHeaderFields"}) as unknown as TypedDocumentString<RecordHeaderFieldsFragment, unknown>;
export const RecordFieldsFragmentDoc = new TypedDocumentString(`
    fragment RecordFields on Record {
  topic
  partition
  offset
  timestamp
  key
  value
  schemaId
  sizeBytes
  compression
  headers {
    ...RecordHeaderFields
  }
}
    fragment RecordHeaderFields on RecordHeader {
  key
  value
}`, {"fragmentName":"RecordFields"}) as unknown as TypedDocumentString<RecordFieldsFragment, unknown>;
export const SearchHitFieldsFragmentDoc = new TypedDocumentString(`
    fragment SearchHitFields on SearchHit {
  kind
  id
  label
  detail
}
    `, {"fragmentName":"SearchHitFields"}) as unknown as TypedDocumentString<SearchHitFieldsFragment, unknown>;
export const WhoamiDocument = new TypedDocumentString(`
    query Whoami {
  whoami {
    ...IdentityFields
  }
}
    fragment IdentityFields on Identity {
  subject
  clusters {
    cluster
    roles
    privileges
  }
}`, {"operationName":"Whoami"}) as unknown as TypedDocumentString<WhoamiQuery, WhoamiQueryVariables>;
export const ClustersDocument = new TypedDocumentString(`
    query Clusters {
  clusters {
    name
    health {
      ...ClusterHealthFields
    }
  }
}
    fragment LaneHealthFields on LaneHealth {
  updatedAt
  checkedAt
  lastError
  lastPollMs
  healthy
}
fragment ClusterHealthFields on ClusterHealth {
  cluster
  ready
  topology {
    ...LaneHealthFields
  }
  watermarks {
    ...LaneHealthFields
  }
  offsets {
    ...LaneHealthFields
  }
  configs {
    ...LaneHealthFields
  }
  subjects {
    ...LaneHealthFields
  }
  topicCount
  partitionCount
  groupCount
  brokerCount
  subjectCount
  underReplicatedPartitions
  offlinePartitions
}`, {"operationName":"Clusters"}) as unknown as TypedDocumentString<ClustersQuery, ClustersQueryVariables>;
export const TopicRowsDocument = new TypedDocumentString(`
    query TopicRows($cluster: String!) {
  cluster(name: $cluster) {
    topics {
      rows {
        ...TopicRowFields
      }
    }
  }
}
    fragment TopicRowFields on TopicRow {
  name
  internal
  partitionCount
  replicationFactor
  retainedMessages
  producedTotal
  rate
  retentionMs
  cleanupPolicy
  groupCount
  underReplicated
}`, {"operationName":"TopicRows"}) as unknown as TypedDocumentString<TopicRowsQuery, TopicRowsQueryVariables>;
export const TopicDocument = new TypedDocumentString(`
    query Topic($cluster: String!, $name: String!) {
  cluster(name: $cluster) {
    topic(name: $name) {
      ...TopicDetailFields
    }
    topics(filter: { contains: $name }) {
      rows {
        ...TopicRowFields
      }
    }
  }
}
    fragment TopicRowFields on TopicRow {
  name
  internal
  partitionCount
  replicationFactor
  retainedMessages
  producedTotal
  rate
  retentionMs
  cleanupPolicy
  groupCount
  underReplicated
}
fragment PartitionRowFields on PartitionRow {
  id
  leader
  replicas
  isr
  lowWatermark
  highWatermark
  retained
  underReplicated
}
fragment TopicDetailFields on TopicDetail {
  name
  internal
  replicationFactor
  retainedMessages
  producedTotal
  groupCount
  underReplicated
  partitions {
    ...PartitionRowFields
  }
}`, {"operationName":"Topic"}) as unknown as TypedDocumentString<TopicQuery, TopicQueryVariables>;
export const TopicGroupsDocument = new TypedDocumentString(`
    query TopicGroups($cluster: String!, $topic: String!) {
  cluster(name: $cluster) {
    topicGroups(topic: $topic) {
      ...TopicGroupRowFields
    }
  }
}
    fragment TopicGroupRowFields on TopicGroupRow {
  id
  state
  memberCount
  lagOnTopic
}`, {"operationName":"TopicGroups"}) as unknown as TypedDocumentString<TopicGroupsQuery, TopicGroupsQueryVariables>;
export const TopicConfigsDocument = new TypedDocumentString(`
    query TopicConfigs($cluster: String!, $name: String!) {
  cluster(name: $cluster) {
    topicConfigs(name: $name) {
      ...ConfigEntryFields
    }
  }
}
    fragment ConfigEntryFields on ConfigEntry {
  name
  value
  source
  readOnly
  sensitive
}`, {"operationName":"TopicConfigs"}) as unknown as TypedDocumentString<TopicConfigsQuery, TopicConfigsQueryVariables>;
export const GroupRowsDocument = new TypedDocumentString(`
    query GroupRows($cluster: String!) {
  cluster(name: $cluster) {
    groups {
      rows {
        ...GroupRowFields
      }
    }
  }
}
    fragment GroupRowFields on GroupRow {
  id
  state
  memberCount
  topicNames
  totalLag
  lagComplete
  coordinatorId
}`, {"operationName":"GroupRows"}) as unknown as TypedDocumentString<GroupRowsQuery, GroupRowsQueryVariables>;
export const GroupDocument = new TypedDocumentString(`
    query Group($cluster: String!, $id: String!) {
  cluster(name: $cluster) {
    group(id: $id) {
      ...GroupDetailFields
    }
  }
}
    fragment MemberAssignmentFields on MemberAssignment {
  topic
  partitions
}
fragment GroupMemberFields on GroupMember {
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
fragment GroupDetailFields on GroupDetail {
  id
  state
  protocol
  coordinatorId
  totalLag
  lagComplete
  members {
    ...GroupMemberFields
  }
  offsets {
    ...GroupOffsetFields
  }
}`, {"operationName":"Group"}) as unknown as TypedDocumentString<GroupQuery, GroupQueryVariables>;
export const BrokerRowsDocument = new TypedDocumentString(`
    query BrokerRows($cluster: String!) {
  cluster(name: $cluster) {
    brokers {
      ...BrokerRowFields
    }
  }
}
    fragment BrokerRowFields on BrokerRow {
  id
  host
  port
  rack
  controller
  partitionCount
  leaderCount
}`, {"operationName":"BrokerRows"}) as unknown as TypedDocumentString<BrokerRowsQuery, BrokerRowsQueryVariables>;
export const BrokerConfigsDocument = new TypedDocumentString(`
    query BrokerConfigs($cluster: String!, $id: Int!) {
  cluster(name: $cluster) {
    brokerConfigs(id: $id) {
      ...ConfigEntryFields
    }
  }
}
    fragment ConfigEntryFields on ConfigEntry {
  name
  value
  source
  readOnly
  sensitive
}`, {"operationName":"BrokerConfigs"}) as unknown as TypedDocumentString<BrokerConfigsQuery, BrokerConfigsQueryVariables>;
export const SubjectRowsDocument = new TypedDocumentString(`
    query SubjectRows($cluster: String!) {
  cluster(name: $cluster) {
    subjects {
      rows {
        ...SubjectRowFields
      }
      sourceHealth {
        ...LaneHealthFields
      }
    }
  }
}
    fragment LaneHealthFields on LaneHealth {
  updatedAt
  checkedAt
  lastError
  lastPollMs
  healthy
}
fragment SubjectRowFields on SubjectRow {
  subject
  id
  type
  latestVersion
  versions
  compatibility
}`, {"operationName":"SubjectRows"}) as unknown as TypedDocumentString<SubjectRowsQuery, SubjectRowsQueryVariables>;
export const SubjectDocument = new TypedDocumentString(`
    query Subject($cluster: String!, $name: String!, $version: Int) {
  cluster(name: $cluster) {
    subject(name: $name, version: $version) {
      ...SubjectDetailFields
    }
  }
}
    fragment SubjectDetailFields on SubjectDetail {
  subject
  version
  id
  type
  schema
  references {
    name
    subject
    version
  }
}`, {"operationName":"Subject"}) as unknown as TypedDocumentString<SubjectQuery, SubjectQueryVariables>;
export const AclsDocument = new TypedDocumentString(`
    query Acls($cluster: String!) {
  cluster(name: $cluster) {
    acls {
      authorizer
      bindings {
        ...AclFields
      }
    }
  }
}
    fragment AclFields on Acl {
  resourceType
  resourceName
  patternType
  principal
  host
  operation
  permission
}`, {"operationName":"Acls"}) as unknown as TypedDocumentString<AclsQuery, AclsQueryVariables>;
export const RecordsDocument = new TypedDocumentString(`
    query Records($cluster: String!, $query: RecordQueryInput!) {
  cluster(name: $cluster) {
    records(query: $query) {
      complete
      obfuscated
      nextCursor
      prevCursor
      records {
        ...RecordFields
      }
    }
  }
}
    fragment RecordHeaderFields on RecordHeader {
  key
  value
}
fragment RecordFields on Record {
  topic
  partition
  offset
  timestamp
  key
  value
  schemaId
  sizeBytes
  compression
  headers {
    ...RecordHeaderFields
  }
}`, {"operationName":"Records"}) as unknown as TypedDocumentString<RecordsQuery, RecordsQueryVariables>;
export const SearchDocument = new TypedDocumentString(`
    query Search($cluster: String!, $term: String!) {
  cluster(name: $cluster) {
    search(term: $term) {
      ...SearchHitFields
    }
  }
}
    fragment SearchHitFields on SearchHit {
  kind
  id
  label
  detail
}`, {"operationName":"Search"}) as unknown as TypedDocumentString<SearchQuery, SearchQueryVariables>;
export const UpdatesDocument = new TypedDocumentString(`
    subscription Updates($cluster: String!, $scope: UpdateScope) {
  updates(cluster: $cluster, scope: $scope) {
    __typename
    ... on WatermarksTick {
      topics {
        topic
        rate
      }
    }
    ... on GroupLagUpdate {
      group
      lag
      lagComplete
      offsets {
        ...GroupOffsetFields
      }
    }
    ... on TopologyDelta {
      version
      addedTopics
      removedTopics
      changedTopics
      addedGroups
      removedGroups
      changedGroups
      brokersChanged
    }
    ... on ConfigsChanged {
      version
      configTopics: topics
    }
    ... on SubjectsChanged {
      version
      added
      removed
      changed
    }
    ... on Resync {
      reason
    }
  }
}
    fragment GroupOffsetFields on GroupOffset {
  topic
  partition
  currentOffset
  endOffset
  lag
  memberId
}`, {"operationName":"Updates"}) as unknown as TypedDocumentString<UpdatesSubscription, UpdatesSubscriptionVariables>;