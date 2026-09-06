export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: { input: string; output: string; }
  String: { input: string; output: string; }
  Boolean: { input: boolean; output: boolean; }
  Int: { input: number; output: number; }
  Float: { input: number; output: number; }
};

export type Acl = {
  host: Scalars['String']['output'];
  operation: Scalars['String']['output'];
  patternType: AclPatternType;
  permission: AclPermission;
  principal: Scalars['String']['output'];
  resourceName: Scalars['String']['output'];
  resourceType: AclResourceType;
};

export type AclPatternType =
  | 'LITERAL'
  | 'PREFIXED';

export type AclPermission =
  | 'ALLOW'
  | 'DENY';

export type AclResourceType =
  | 'CLUSTER'
  | 'GROUP'
  | 'TOPIC'
  | 'TRANSACTIONAL_ID';

export type Broker = {
  bytesInPerSec: Scalars['Float']['output'];
  bytesOutPerSec: Scalars['Float']['output'];
  controller: Scalars['Boolean']['output'];
  host: Scalars['String']['output'];
  id: Scalars['Int']['output'];
  leaderCount: Scalars['Int']['output'];
  logDirSizeBytes: Scalars['Float']['output'];
  partitionCount: Scalars['Int']['output'];
  port: Scalars['Int']['output'];
  rack: Maybe<Scalars['String']['output']>;
};

export type CleanupPolicy =
  | 'compact'
  | 'compact_delete'
  | 'delete';

export type Cluster = {
  bootstrapServers: Array<Scalars['String']['output']>;
  brokerCount: Scalars['Int']['output'];
  bytesInPerSec: Scalars['Float']['output'];
  bytesOutPerSec: Scalars['Float']['output'];
  clusterId: Scalars['String']['output'];
  consumerGroupCount: Scalars['Int']['output'];
  label: Scalars['String']['output'];
  messageCount: Scalars['Float']['output'];
  name: Scalars['String']['output'];
  offlinePartitions: Scalars['Int']['output'];
  partitionCount: Scalars['Int']['output'];
  securityProtocol: SecurityProtocol;
  sizeBytes: Scalars['Float']['output'];
  status: ClusterStatus;
  topicCount: Scalars['Int']['output'];
  underReplicatedPartitions: Scalars['Int']['output'];
  version: Scalars['String']['output'];
};

export type ClusterStatus =
  | 'degraded'
  | 'healthy'
  | 'offline';

export type Compression =
  | 'gzip'
  | 'lz4'
  | 'none'
  | 'snappy'
  | 'zstd';

export type ConfigEntry = {
  documentation: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  readOnly: Scalars['Boolean']['output'];
  sensitive: Scalars['Boolean']['output'];
  source: ConfigSource;
  value: Maybe<Scalars['String']['output']>;
};

export type ConfigSource =
  | 'DEFAULT_CONFIG'
  | 'DYNAMIC_BROKER_CONFIG'
  | 'DYNAMIC_TOPIC_CONFIG'
  | 'STATIC_BROKER_CONFIG';

export type ConsumerGroup = {
  coordinator: Scalars['Int']['output'];
  id: Scalars['String']['output'];
  lag: Scalars['Float']['output'];
  members: Array<ConsumerGroupMember>;
  offsets: Array<GroupOffset>;
  protocol: Scalars['String']['output'];
  state: ConsumerGroupState;
  topics: Array<Scalars['String']['output']>;
};

export type ConsumerGroupMember = {
  assignments: Array<MemberAssignment>;
  clientId: Scalars['String']['output'];
  host: Scalars['String']['output'];
  id: Scalars['String']['output'];
};

export type ConsumerGroupState =
  | 'CompletingRebalance'
  | 'Dead'
  | 'Empty'
  | 'PreparingRebalance'
  | 'Stable';

export type GroupOffset = {
  currentOffset: Scalars['Float']['output'];
  endOffset: Scalars['Float']['output'];
  lag: Scalars['Float']['output'];
  memberId: Maybe<Scalars['String']['output']>;
  partition: Scalars['Int']['output'];
  topic: Scalars['String']['output'];
};

export type MemberAssignment = {
  partitions: Array<Scalars['Int']['output']>;
  topic: Scalars['String']['output'];
};

export type Partition = {
  highWatermark: Scalars['Float']['output'];
  id: Scalars['Int']['output'];
  isr: Array<Scalars['Int']['output']>;
  leader: Scalars['Int']['output'];
  lowWatermark: Scalars['Float']['output'];
  replicas: Array<Scalars['Int']['output']>;
  sizeBytes: Scalars['Float']['output'];
};

export type Query = {
  acls: Array<Acl>;
  broker: Maybe<Broker>;
  brokerConfigs: Array<ConfigEntry>;
  brokers: Array<Broker>;
  cluster: Maybe<Cluster>;
  clusterThroughput: Array<ThroughputPoint>;
  clusters: Array<Cluster>;
  consumerGroup: Maybe<ConsumerGroup>;
  consumerGroups: Array<ConsumerGroup>;
  records: Array<TopicRecord>;
  schemaSubjects: Array<SchemaSubject>;
  search: Array<SearchResult>;
  topic: Maybe<Topic>;
  topicConfigs: Array<ConfigEntry>;
  topicThroughput: Array<ThroughputPoint>;
  topics: Array<Topic>;
};


export type QueryAclsArgs = {
  cluster: Scalars['String']['input'];
};


export type QueryBrokerArgs = {
  cluster: Scalars['String']['input'];
  id: Scalars['Int']['input'];
};


export type QueryBrokerConfigsArgs = {
  cluster: Scalars['String']['input'];
  id: Scalars['Int']['input'];
};


export type QueryBrokersArgs = {
  cluster: Scalars['String']['input'];
};


export type QueryClusterArgs = {
  name: Scalars['String']['input'];
};


export type QueryClusterThroughputArgs = {
  cluster: Scalars['String']['input'];
};


export type QueryConsumerGroupArgs = {
  cluster: Scalars['String']['input'];
  id: Scalars['String']['input'];
};


export type QueryConsumerGroupsArgs = {
  cluster: Scalars['String']['input'];
};


export type QueryRecordsArgs = {
  query: RecordQuery;
};


export type QuerySchemaSubjectsArgs = {
  cluster: Scalars['String']['input'];
};


export type QuerySearchArgs = {
  cluster: Scalars['String']['input'];
  term: Scalars['String']['input'];
};


export type QueryTopicArgs = {
  cluster: Scalars['String']['input'];
  name: Scalars['String']['input'];
};


export type QueryTopicConfigsArgs = {
  cluster: Scalars['String']['input'];
  name: Scalars['String']['input'];
};


export type QueryTopicThroughputArgs = {
  cluster: Scalars['String']['input'];
  topic: Scalars['String']['input'];
};


export type QueryTopicsArgs = {
  cluster: Scalars['String']['input'];
};

export type RecordHeader = {
  key: Scalars['String']['output'];
  value: Scalars['String']['output'];
};

export type RecordOrder =
  | 'newest'
  | 'oldest';

export type RecordQuery = {
  cluster: Scalars['String']['input'];
  limit: Scalars['Int']['input'];
  order: RecordOrder;
  partition: InputMaybe<Scalars['Int']['input']>;
  search: Scalars['String']['input'];
  topic: Scalars['String']['input'];
};

export type SchemaCompatibility =
  | 'BACKWARD'
  | 'FORWARD'
  | 'FULL'
  | 'NONE';

export type SchemaSubject = {
  compatibility: SchemaCompatibility;
  id: Scalars['Int']['output'];
  latestVersion: Scalars['Int']['output'];
  schema: Scalars['String']['output'];
  subject: Scalars['String']['output'];
  type: SchemaType;
  versions: Array<Scalars['Int']['output']>;
};

export type SchemaType =
  | 'AVRO'
  | 'JSON'
  | 'PROTOBUF';

export type SearchResult = {
  detail: Scalars['String']['output'];
  id: Scalars['String']['output'];
  kind: SearchResultKind;
  label: Scalars['String']['output'];
};

export type SearchResultKind =
  | 'group'
  | 'node'
  | 'subject'
  | 'topic';

export type SecurityProtocol =
  | 'PLAINTEXT'
  | 'SASL_PLAINTEXT'
  | 'SASL_SSL'
  | 'SSL';

export type ThroughputPoint = {
  bytesIn: Scalars['Float']['output'];
  bytesOut: Scalars['Float']['output'];
  messages: Scalars['Float']['output'];
  timestamp: Scalars['Float']['output'];
};

export type Topic = {
  bytesInPerSec: Scalars['Float']['output'];
  cleanupPolicy: CleanupPolicy;
  consumerGroups: Array<Scalars['String']['output']>;
  internal: Scalars['Boolean']['output'];
  messageCount: Scalars['Float']['output'];
  messagesPerSec: Scalars['Float']['output'];
  name: Scalars['String']['output'];
  partitions: Array<Partition>;
  replicationFactor: Scalars['Int']['output'];
  retentionMs: Scalars['Float']['output'];
  sizeBytes: Scalars['Float']['output'];
  underReplicated: Scalars['Boolean']['output'];
};

export type TopicRecord = {
  compression: Compression;
  headers: Array<RecordHeader>;
  key: Maybe<Scalars['String']['output']>;
  offset: Scalars['Float']['output'];
  partition: Scalars['Int']['output'];
  sizeBytes: Scalars['Float']['output'];
  timestamp: Scalars['Float']['output'];
  topic: Scalars['String']['output'];
  value: Maybe<Scalars['String']['output']>;
};
