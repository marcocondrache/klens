export type ClusterStatus = "healthy" | "degraded" | "offline"

export type Environment = "development" | "staging" | "production"

export interface Cluster {
  name: string
  label: string
  environment: Environment
  clusterId: string
  bootstrapServers: string[]
  securityProtocol: "PLAINTEXT" | "SSL" | "SASL_SSL"
  version: string
  status: ClusterStatus
  brokerCount: number
  topicCount: number
  partitionCount: number
  consumerGroupCount: number
  underReplicatedPartitions: number
  offlinePartitions: number
  messageCount: number
  sizeBytes: number
  bytesInPerSec: number
  bytesOutPerSec: number
}

export interface Broker {
  id: number
  host: string
  port: number
  rack: string | null
  controller: boolean
  partitionCount: number
  leaderCount: number
  logDirSizeBytes: number
  bytesInPerSec: number
  bytesOutPerSec: number
}

export interface Partition {
  id: number
  leader: number
  replicas: number[]
  isr: number[]
  lowWatermark: number
  highWatermark: number
  sizeBytes: number
}

export type CleanupPolicy = "delete" | "compact" | "compact,delete"

export interface Topic {
  name: string
  internal: boolean
  partitions: Partition[]
  replicationFactor: number
  messageCount: number
  sizeBytes: number
  cleanupPolicy: CleanupPolicy
  retentionMs: number
  consumerGroups: string[]
  bytesInPerSec: number
  messagesPerSec: number
  underReplicated: boolean
}

export type ConfigSource =
  | "DYNAMIC_TOPIC_CONFIG"
  | "DYNAMIC_BROKER_CONFIG"
  | "STATIC_BROKER_CONFIG"
  | "DEFAULT_CONFIG"

export interface ConfigEntry {
  name: string
  value: string | null
  source: ConfigSource
  readOnly: boolean
  sensitive: boolean
  documentation: string | null
}

export type ConsumerGroupState =
  | "Stable"
  | "Empty"
  | "PreparingRebalance"
  | "CompletingRebalance"
  | "Dead"

export interface MemberAssignment {
  topic: string
  partitions: number[]
}

export interface ConsumerGroupMember {
  id: string
  clientId: string
  host: string
  assignments: MemberAssignment[]
}

export interface GroupOffset {
  topic: string
  partition: number
  currentOffset: number
  endOffset: number
  lag: number
  memberId: string | null
}

export interface ConsumerGroup {
  id: string
  state: ConsumerGroupState
  protocol: string
  coordinator: number
  members: ConsumerGroupMember[]
  topics: string[]
  lag: number
  offsets: GroupOffset[]
}

export interface RecordHeader {
  key: string
  value: string
}

export interface TopicRecord {
  topic: string
  partition: number
  offset: number
  timestamp: number
  key: string | null
  value: string | null
  headers: RecordHeader[]
  sizeBytes: number
  compression: "none" | "gzip" | "snappy" | "lz4" | "zstd"
}

export interface RecordQuery {
  cluster: string
  topic: string
  partition: number | "all"
  search: string
  limit: number
  order: "newest" | "oldest"
}

export interface ThroughputPoint {
  timestamp: number
  bytesIn: number
  bytesOut: number
  messages: number
}

export interface SchemaSubject {
  subject: string
  id: number
  type: "AVRO" | "JSON" | "PROTOBUF"
  latestVersion: number
  versions: number[]
  compatibility: "BACKWARD" | "FORWARD" | "FULL" | "NONE"
  schema: string
}

export interface Acl {
  principal: string
  resourceType: "TOPIC" | "GROUP" | "CLUSTER" | "TRANSACTIONAL_ID"
  resourceName: string
  patternType: "LITERAL" | "PREFIXED"
  operation: string
  permission: "ALLOW" | "DENY"
  host: string
}

export type SearchResultKind = "topic" | "group" | "node" | "subject"

export interface SearchResult {
  kind: SearchResultKind
  id: string
  label: string
  detail: string
  href: string
}
