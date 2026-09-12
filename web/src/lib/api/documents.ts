import { graphql } from "@/graphql/gql";

export const ClusterFields = graphql(`
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
`);

export const BrokerFields = graphql(`
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
`);

export const PartitionFields = graphql(`
  fragment PartitionFields on Partition {
    id
    leader
    replicas
    isr
    lowWatermark
    highWatermark
    sizeBytes
  }
`);

export const TopicFields = graphql(`
  fragment TopicFields on Topic {
    name
    internal
    partitions {
      ...PartitionFields
    }
    partitionCount
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
`);

export const TopicListFields = graphql(`
  fragment TopicListFields on Topic {
    name
    internal
    partitionCount
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
`);

export const ConfigEntryFields = graphql(`
  fragment ConfigEntryFields on ConfigEntry {
    name
    value
    source
    readOnly
    sensitive
    documentation
  }
`);

export const MemberAssignmentFields = graphql(`
  fragment MemberAssignmentFields on MemberAssignment {
    topic
    partitions
  }
`);

export const ConsumerGroupMemberFields = graphql(`
  fragment ConsumerGroupMemberFields on ConsumerGroupMember {
    id
    clientId
    host
    assignments {
      ...MemberAssignmentFields
    }
  }
`);

export const GroupOffsetFields = graphql(`
  fragment GroupOffsetFields on GroupOffset {
    topic
    partition
    currentOffset
    endOffset
    lag
    memberId
  }
`);

export const ConsumerGroupFields = graphql(`
  fragment ConsumerGroupFields on ConsumerGroup {
    id
    state
    protocol
    coordinator
    members {
      ...ConsumerGroupMemberFields
    }
    memberCount
    topics
    lag
    offsets {
      ...GroupOffsetFields
    }
    assignedPartitionCount
  }
`);

export const GroupListFields = graphql(`
  fragment GroupListFields on ConsumerGroup {
    id
    state
    protocol
    coordinator
    memberCount
    topics
    lag
    assignedPartitionCount
  }
`);

export const ThroughputPointFields = graphql(`
  fragment ThroughputPointFields on ThroughputPoint {
    timestamp
    bytesIn
    bytesOut
    messages
  }
`);

export const TopicRateFields = graphql(`
  fragment TopicRateFields on TopicRate {
    name
    messagesPerSec
    bytesInPerSec
  }
`);

export const ConsumerGroupLagFields = graphql(`
  fragment ConsumerGroupLagFields on ConsumerGroup {
    id
    lag
    offsets {
      ...GroupOffsetFields
    }
  }
`);

export const SchemaSubjectFields = graphql(`
  fragment SchemaSubjectFields on SchemaSubject {
    subject
    id
    type
    latestVersion
    versions
    compatibility
    schema
  }
`);

export const RecordHeaderFields = graphql(`
  fragment RecordHeaderFields on RecordHeader {
    key
    value
  }
`);

export const TopicRecordFields = graphql(`
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
`);

export const SearchResultFields = graphql(`
  fragment SearchResultFields on SearchResult {
    kind
    id
    label
    detail
  }
`);

export const clustersQuery = graphql(`
  query Clusters {
    clusters {
      ...ClusterFields
    }
  }
`);

export const clusterQuery = graphql(`
  query Cluster($name: String!) {
    cluster(name: $name) {
      ...ClusterFields
    }
  }
`);

export const catalogHealthQuery = graphql(`
  query CatalogHealth($cluster: String!) {
    catalogHealth(cluster: $cluster) {
      updatedAt
      subjectsUpdatedAt
      lastError
      lastPollDurationMs
      topicCount
      groupCount
      brokerCount
      subjectCount
    }
  }
`);

export const brokersQuery = graphql(`
  query Brokers($cluster: String!) {
    brokers(cluster: $cluster) {
      ...BrokerFields
    }
  }
`);

export const brokerQuery = graphql(`
  query Broker($cluster: String!, $id: Int!) {
    broker(cluster: $cluster, id: $id) {
      ...BrokerFields
    }
  }
`);

export const brokerConfigsQuery = graphql(`
  query BrokerConfigs($cluster: String!, $id: Int!) {
    brokerConfigs(cluster: $cluster, id: $id) {
      ...ConfigEntryFields
    }
  }
`);

export const topicsQuery = graphql(`
  query Topics($cluster: String!) {
    clusterCatalog(cluster: $cluster) {
      updatedAt
      topics {
        ...TopicListFields
      }
    }
  }
`);

export const topicQuery = graphql(`
  query Topic($cluster: String!, $name: String!) {
    topic(cluster: $cluster, name: $name) {
      ...TopicFields
    }
  }
`);

export const topicConfigsQuery = graphql(`
  query TopicConfigs($cluster: String!, $name: String!) {
    topicConfigs(cluster: $cluster, name: $name) {
      ...ConfigEntryFields
    }
  }
`);

export const consumerGroupsQuery = graphql(`
  query ConsumerGroups($cluster: String!, $topic: String) {
    consumerGroups(cluster: $cluster, topic: $topic) {
      ...ConsumerGroupFields
    }
  }
`);

export const groupsCatalogQuery = graphql(`
  query GroupsCatalog($cluster: String!) {
    clusterCatalog(cluster: $cluster) {
      updatedAt
      consumerGroups {
        ...GroupListFields
      }
    }
  }
`);

export const consumerGroupQuery = graphql(`
  query ConsumerGroup($cluster: String!, $id: String!) {
    consumerGroup(cluster: $cluster, id: $id) {
      ...ConsumerGroupFields
    }
  }
`);

export const clusterThroughputQuery = graphql(`
  query ClusterThroughput($cluster: String!) {
    clusterThroughput(cluster: $cluster) {
      ...ThroughputPointFields
    }
  }
`);

export const topicThroughputQuery = graphql(`
  query TopicThroughput($cluster: String!, $topic: String!) {
    topicThroughput(cluster: $cluster, topic: $topic) {
      ...ThroughputPointFields
    }
  }
`);

export const groupLagHistoryQuery = graphql(`
  query GroupLagHistory($cluster: String!, $id: String!) {
    groupLagHistory(cluster: $cluster, id: $id) {
      ...ThroughputPointFields
    }
  }
`);

export const schemaSubjectsQuery = graphql(`
  query SchemaSubjects($cluster: String!) {
    schemaSubjects(cluster: $cluster) {
      ...SchemaSubjectFields
    }
  }
`);

export const recordsQuery = graphql(`
  query Records($query: RecordQuery!) {
    records(query: $query) {
      records {
        ...TopicRecordFields
      }
      hasMore
      nextCursor
    }
  }
`);

export const searchQuery = graphql(`
  query Search($cluster: String!, $term: String!) {
    search(cluster: $cluster, term: $term) {
      ...SearchResultFields
    }
  }
`);

export const topicRatesSubscription = graphql(`
  subscription TopicRates($cluster: String!) {
    topicRates(cluster: $cluster) {
      ...TopicRateFields
    }
  }
`);

export const consumerGroupLagSubscription = graphql(`
  subscription ConsumerGroupLag($cluster: String!, $id: String!) {
    consumerGroupLag(cluster: $cluster, id: $id) {
      ...ConsumerGroupLagFields
    }
  }
`);
