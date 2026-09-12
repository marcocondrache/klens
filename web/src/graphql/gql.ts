/* eslint-disable */
import * as types from "./graphql";

/**
 * Map of all GraphQL operations in the project.
 *
 * This map has several performance disadvantages:
 * 1. It is not tree-shakeable, so it will include all operations in the project.
 * 2. It is not minifiable, so the string of a GraphQL query will be multiple times inside the bundle.
 * 3. It does not support dead code elimination, so it will add unused operations.
 *
 * Therefore it is highly recommended to use the babel or swc plugin for production.
 * Learn more about it here: https://the-guild.dev/graphql/codegen/plugins/presets/preset-client#reducing-bundle-size
 */
type Documents = {
  "\n  fragment ClusterFields on Cluster {\n    name\n    label\n    clusterId\n    bootstrapServers\n    securityProtocol\n    status\n    brokerCount\n    topicCount\n    partitionCount\n    consumerGroupCount\n    underReplicatedPartitions\n    offlinePartitions\n    messageCount\n  }\n": typeof types.ClusterFieldsFragmentDoc;
  "\n  fragment BrokerFields on Broker {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n  }\n": typeof types.BrokerFieldsFragmentDoc;
  "\n  fragment PartitionFields on Partition {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n  }\n": typeof types.PartitionFieldsFragmentDoc;
  "\n  fragment TopicFields on Topic {\n    name\n    internal\n    partitions {\n      ...PartitionFields\n    }\n    partitionCount\n    replicationFactor\n    messageCount\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    messagesPerSec\n    underReplicated\n  }\n": typeof types.TopicFieldsFragmentDoc;
  "\n  fragment TopicListFields on Topic {\n    name\n    internal\n    partitionCount\n    replicationFactor\n    messageCount\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    messagesPerSec\n    underReplicated\n  }\n": typeof types.TopicListFieldsFragmentDoc;
  "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n  }\n": typeof types.ConfigEntryFieldsFragmentDoc;
  "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n": typeof types.MemberAssignmentFieldsFragmentDoc;
  "\n  fragment ConsumerGroupMemberFields on ConsumerGroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n": typeof types.ConsumerGroupMemberFieldsFragmentDoc;
  "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n": typeof types.GroupOffsetFieldsFragmentDoc;
  "\n  fragment ConsumerGroupFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    members {\n      ...ConsumerGroupMemberFields\n    }\n    memberCount\n    topics\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n    assignedPartitionCount\n  }\n": typeof types.ConsumerGroupFieldsFragmentDoc;
  "\n  fragment GroupListFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    memberCount\n    topics\n    lag\n    assignedPartitionCount\n  }\n": typeof types.GroupListFieldsFragmentDoc;
  "\n  fragment ThroughputPointFields on ThroughputPoint {\n    timestamp\n    messages\n  }\n": typeof types.ThroughputPointFieldsFragmentDoc;
  "\n  fragment TopicRateFields on TopicRate {\n    name\n    messagesPerSec\n  }\n": typeof types.TopicRateFieldsFragmentDoc;
  "\n  fragment ConsumerGroupLagFields on ConsumerGroup {\n    id\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n": typeof types.ConsumerGroupLagFieldsFragmentDoc;
  "\n  fragment SchemaSubjectFields on SchemaSubject {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n    schema\n  }\n": typeof types.SchemaSubjectFieldsFragmentDoc;
  "\n  fragment RecordHeaderFields on RecordHeader {\n    key\n    value\n  }\n": typeof types.RecordHeaderFieldsFragmentDoc;
  "\n  fragment TopicRecordFields on TopicRecord {\n    topic\n    partition\n    offset\n    timestamp\n    key\n    value\n    schemaId\n    headers {\n      ...RecordHeaderFields\n    }\n    sizeBytes\n    compression\n  }\n": typeof types.TopicRecordFieldsFragmentDoc;
  "\n  fragment SearchResultFields on SearchResult {\n    kind\n    id\n    label\n    detail\n  }\n": typeof types.SearchResultFieldsFragmentDoc;
  "\n  query Clusters {\n    clusters {\n      ...ClusterFields\n    }\n  }\n": typeof types.ClustersDocument;
  "\n  query Cluster($name: String!) {\n    cluster(name: $name) {\n      ...ClusterFields\n    }\n  }\n": typeof types.ClusterDocument;
  "\n  query CatalogHealth($cluster: String!) {\n    catalogHealth(cluster: $cluster) {\n      updatedAt\n      subjectsUpdatedAt\n      lastError\n      lastPollDurationMs\n      topicCount\n      groupCount\n      brokerCount\n      subjectCount\n    }\n  }\n": typeof types.CatalogHealthDocument;
  "\n  query Brokers($cluster: String!) {\n    brokers(cluster: $cluster) {\n      ...BrokerFields\n    }\n  }\n": typeof types.BrokersDocument;
  "\n  query Broker($cluster: String!, $id: Int!) {\n    broker(cluster: $cluster, id: $id) {\n      ...BrokerFields\n    }\n  }\n": typeof types.BrokerDocument;
  "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n": typeof types.BrokerConfigsDocument;
  "\n  query Topics($cluster: String!) {\n    clusterCatalog(cluster: $cluster) {\n      updatedAt\n      topics {\n        ...TopicListFields\n      }\n    }\n  }\n": typeof types.TopicsDocument;
  "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicFields\n    }\n  }\n": typeof types.TopicDocument;
  "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n": typeof types.TopicConfigsDocument;
  "\n  query ConsumerGroups($cluster: String!, $topic: String) {\n    consumerGroups(cluster: $cluster, topic: $topic) {\n      ...ConsumerGroupFields\n    }\n  }\n": typeof types.ConsumerGroupsDocument;
  "\n  query GroupsCatalog($cluster: String!) {\n    clusterCatalog(cluster: $cluster) {\n      updatedAt\n      consumerGroups {\n        ...GroupListFields\n      }\n    }\n  }\n": typeof types.GroupsCatalogDocument;
  "\n  query ConsumerGroup($cluster: String!, $id: String!) {\n    consumerGroup(cluster: $cluster, id: $id) {\n      ...ConsumerGroupFields\n    }\n  }\n": typeof types.ConsumerGroupDocument;
  "\n  query TopicThroughput($cluster: String!, $topic: String!) {\n    topicThroughput(cluster: $cluster, topic: $topic) {\n      ...ThroughputPointFields\n    }\n  }\n": typeof types.TopicThroughputDocument;
  "\n  query GroupLagHistory($cluster: String!, $id: String!) {\n    groupLagHistory(cluster: $cluster, id: $id) {\n      ...ThroughputPointFields\n    }\n  }\n": typeof types.GroupLagHistoryDocument;
  "\n  query SchemaSubjects($cluster: String!) {\n    schemaSubjects(cluster: $cluster) {\n      ...SchemaSubjectFields\n    }\n  }\n": typeof types.SchemaSubjectsDocument;
  "\n  query Records($query: RecordQuery!) {\n    records(query: $query) {\n      records {\n        ...TopicRecordFields\n      }\n      hasMore\n      nextCursor\n    }\n  }\n": typeof types.RecordsDocument;
  "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      hits {\n        ...SearchResultFields\n      }\n      schemaRegistryError\n    }\n  }\n": typeof types.SearchDocument;
  "\n  subscription TopicRates($cluster: String!) {\n    topicRates(cluster: $cluster) {\n      ...TopicRateFields\n    }\n  }\n": typeof types.TopicRatesDocument;
  "\n  subscription ConsumerGroupLag($cluster: String!, $id: String!) {\n    consumerGroupLag(cluster: $cluster, id: $id) {\n      ...ConsumerGroupLagFields\n    }\n  }\n": typeof types.ConsumerGroupLagDocument;
  "\n  subscription CatalogUpdated($cluster: String!) {\n    catalogUpdated(cluster: $cluster) {\n      cluster\n      updatedAt\n      generation\n    }\n  }\n": typeof types.CatalogUpdatedDocument;
};
const documents: Documents = {
  "\n  fragment ClusterFields on Cluster {\n    name\n    label\n    clusterId\n    bootstrapServers\n    securityProtocol\n    status\n    brokerCount\n    topicCount\n    partitionCount\n    consumerGroupCount\n    underReplicatedPartitions\n    offlinePartitions\n    messageCount\n  }\n":
    types.ClusterFieldsFragmentDoc,
  "\n  fragment BrokerFields on Broker {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n  }\n":
    types.BrokerFieldsFragmentDoc,
  "\n  fragment PartitionFields on Partition {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n  }\n":
    types.PartitionFieldsFragmentDoc,
  "\n  fragment TopicFields on Topic {\n    name\n    internal\n    partitions {\n      ...PartitionFields\n    }\n    partitionCount\n    replicationFactor\n    messageCount\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    messagesPerSec\n    underReplicated\n  }\n":
    types.TopicFieldsFragmentDoc,
  "\n  fragment TopicListFields on Topic {\n    name\n    internal\n    partitionCount\n    replicationFactor\n    messageCount\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    messagesPerSec\n    underReplicated\n  }\n":
    types.TopicListFieldsFragmentDoc,
  "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n  }\n":
    types.ConfigEntryFieldsFragmentDoc,
  "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n":
    types.MemberAssignmentFieldsFragmentDoc,
  "\n  fragment ConsumerGroupMemberFields on ConsumerGroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n":
    types.ConsumerGroupMemberFieldsFragmentDoc,
  "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n":
    types.GroupOffsetFieldsFragmentDoc,
  "\n  fragment ConsumerGroupFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    members {\n      ...ConsumerGroupMemberFields\n    }\n    memberCount\n    topics\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n    assignedPartitionCount\n  }\n":
    types.ConsumerGroupFieldsFragmentDoc,
  "\n  fragment GroupListFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    memberCount\n    topics\n    lag\n    assignedPartitionCount\n  }\n":
    types.GroupListFieldsFragmentDoc,
  "\n  fragment ThroughputPointFields on ThroughputPoint {\n    timestamp\n    messages\n  }\n":
    types.ThroughputPointFieldsFragmentDoc,
  "\n  fragment TopicRateFields on TopicRate {\n    name\n    messagesPerSec\n  }\n":
    types.TopicRateFieldsFragmentDoc,
  "\n  fragment ConsumerGroupLagFields on ConsumerGroup {\n    id\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n":
    types.ConsumerGroupLagFieldsFragmentDoc,
  "\n  fragment SchemaSubjectFields on SchemaSubject {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n    schema\n  }\n":
    types.SchemaSubjectFieldsFragmentDoc,
  "\n  fragment RecordHeaderFields on RecordHeader {\n    key\n    value\n  }\n":
    types.RecordHeaderFieldsFragmentDoc,
  "\n  fragment TopicRecordFields on TopicRecord {\n    topic\n    partition\n    offset\n    timestamp\n    key\n    value\n    schemaId\n    headers {\n      ...RecordHeaderFields\n    }\n    sizeBytes\n    compression\n  }\n":
    types.TopicRecordFieldsFragmentDoc,
  "\n  fragment SearchResultFields on SearchResult {\n    kind\n    id\n    label\n    detail\n  }\n":
    types.SearchResultFieldsFragmentDoc,
  "\n  query Clusters {\n    clusters {\n      ...ClusterFields\n    }\n  }\n":
    types.ClustersDocument,
  "\n  query Cluster($name: String!) {\n    cluster(name: $name) {\n      ...ClusterFields\n    }\n  }\n":
    types.ClusterDocument,
  "\n  query CatalogHealth($cluster: String!) {\n    catalogHealth(cluster: $cluster) {\n      updatedAt\n      subjectsUpdatedAt\n      lastError\n      lastPollDurationMs\n      topicCount\n      groupCount\n      brokerCount\n      subjectCount\n    }\n  }\n":
    types.CatalogHealthDocument,
  "\n  query Brokers($cluster: String!) {\n    brokers(cluster: $cluster) {\n      ...BrokerFields\n    }\n  }\n":
    types.BrokersDocument,
  "\n  query Broker($cluster: String!, $id: Int!) {\n    broker(cluster: $cluster, id: $id) {\n      ...BrokerFields\n    }\n  }\n":
    types.BrokerDocument,
  "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n":
    types.BrokerConfigsDocument,
  "\n  query Topics($cluster: String!) {\n    clusterCatalog(cluster: $cluster) {\n      updatedAt\n      topics {\n        ...TopicListFields\n      }\n    }\n  }\n":
    types.TopicsDocument,
  "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicFields\n    }\n  }\n":
    types.TopicDocument,
  "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n":
    types.TopicConfigsDocument,
  "\n  query ConsumerGroups($cluster: String!, $topic: String) {\n    consumerGroups(cluster: $cluster, topic: $topic) {\n      ...ConsumerGroupFields\n    }\n  }\n":
    types.ConsumerGroupsDocument,
  "\n  query GroupsCatalog($cluster: String!) {\n    clusterCatalog(cluster: $cluster) {\n      updatedAt\n      consumerGroups {\n        ...GroupListFields\n      }\n    }\n  }\n":
    types.GroupsCatalogDocument,
  "\n  query ConsumerGroup($cluster: String!, $id: String!) {\n    consumerGroup(cluster: $cluster, id: $id) {\n      ...ConsumerGroupFields\n    }\n  }\n":
    types.ConsumerGroupDocument,
  "\n  query TopicThroughput($cluster: String!, $topic: String!) {\n    topicThroughput(cluster: $cluster, topic: $topic) {\n      ...ThroughputPointFields\n    }\n  }\n":
    types.TopicThroughputDocument,
  "\n  query GroupLagHistory($cluster: String!, $id: String!) {\n    groupLagHistory(cluster: $cluster, id: $id) {\n      ...ThroughputPointFields\n    }\n  }\n":
    types.GroupLagHistoryDocument,
  "\n  query SchemaSubjects($cluster: String!) {\n    schemaSubjects(cluster: $cluster) {\n      ...SchemaSubjectFields\n    }\n  }\n":
    types.SchemaSubjectsDocument,
  "\n  query Records($query: RecordQuery!) {\n    records(query: $query) {\n      records {\n        ...TopicRecordFields\n      }\n      hasMore\n      nextCursor\n    }\n  }\n":
    types.RecordsDocument,
  "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      hits {\n        ...SearchResultFields\n      }\n      schemaRegistryError\n    }\n  }\n":
    types.SearchDocument,
  "\n  subscription TopicRates($cluster: String!) {\n    topicRates(cluster: $cluster) {\n      ...TopicRateFields\n    }\n  }\n":
    types.TopicRatesDocument,
  "\n  subscription ConsumerGroupLag($cluster: String!, $id: String!) {\n    consumerGroupLag(cluster: $cluster, id: $id) {\n      ...ConsumerGroupLagFields\n    }\n  }\n":
    types.ConsumerGroupLagDocument,
  "\n  subscription CatalogUpdated($cluster: String!) {\n    catalogUpdated(cluster: $cluster) {\n      cluster\n      updatedAt\n      generation\n    }\n  }\n":
    types.CatalogUpdatedDocument,
};

/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ClusterFields on Cluster {\n    name\n    label\n    clusterId\n    bootstrapServers\n    securityProtocol\n    status\n    brokerCount\n    topicCount\n    partitionCount\n    consumerGroupCount\n    underReplicatedPartitions\n    offlinePartitions\n    messageCount\n  }\n",
): typeof import("./graphql").ClusterFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment BrokerFields on Broker {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n  }\n",
): typeof import("./graphql").BrokerFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment PartitionFields on Partition {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n  }\n",
): typeof import("./graphql").PartitionFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment TopicFields on Topic {\n    name\n    internal\n    partitions {\n      ...PartitionFields\n    }\n    partitionCount\n    replicationFactor\n    messageCount\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    messagesPerSec\n    underReplicated\n  }\n",
): typeof import("./graphql").TopicFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment TopicListFields on Topic {\n    name\n    internal\n    partitionCount\n    replicationFactor\n    messageCount\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    messagesPerSec\n    underReplicated\n  }\n",
): typeof import("./graphql").TopicListFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n  }\n",
): typeof import("./graphql").ConfigEntryFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n",
): typeof import("./graphql").MemberAssignmentFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ConsumerGroupMemberFields on ConsumerGroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n",
): typeof import("./graphql").ConsumerGroupMemberFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n",
): typeof import("./graphql").GroupOffsetFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ConsumerGroupFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    members {\n      ...ConsumerGroupMemberFields\n    }\n    memberCount\n    topics\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n    assignedPartitionCount\n  }\n",
): typeof import("./graphql").ConsumerGroupFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment GroupListFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    memberCount\n    topics\n    lag\n    assignedPartitionCount\n  }\n",
): typeof import("./graphql").GroupListFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ThroughputPointFields on ThroughputPoint {\n    timestamp\n    messages\n  }\n",
): typeof import("./graphql").ThroughputPointFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment TopicRateFields on TopicRate {\n    name\n    messagesPerSec\n  }\n",
): typeof import("./graphql").TopicRateFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ConsumerGroupLagFields on ConsumerGroup {\n    id\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n",
): typeof import("./graphql").ConsumerGroupLagFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment SchemaSubjectFields on SchemaSubject {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n    schema\n  }\n",
): typeof import("./graphql").SchemaSubjectFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment RecordHeaderFields on RecordHeader {\n    key\n    value\n  }\n",
): typeof import("./graphql").RecordHeaderFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment TopicRecordFields on TopicRecord {\n    topic\n    partition\n    offset\n    timestamp\n    key\n    value\n    schemaId\n    headers {\n      ...RecordHeaderFields\n    }\n    sizeBytes\n    compression\n  }\n",
): typeof import("./graphql").TopicRecordFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment SearchResultFields on SearchResult {\n    kind\n    id\n    label\n    detail\n  }\n",
): typeof import("./graphql").SearchResultFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Clusters {\n    clusters {\n      ...ClusterFields\n    }\n  }\n",
): typeof import("./graphql").ClustersDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Cluster($name: String!) {\n    cluster(name: $name) {\n      ...ClusterFields\n    }\n  }\n",
): typeof import("./graphql").ClusterDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query CatalogHealth($cluster: String!) {\n    catalogHealth(cluster: $cluster) {\n      updatedAt\n      subjectsUpdatedAt\n      lastError\n      lastPollDurationMs\n      topicCount\n      groupCount\n      brokerCount\n      subjectCount\n    }\n  }\n",
): typeof import("./graphql").CatalogHealthDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Brokers($cluster: String!) {\n    brokers(cluster: $cluster) {\n      ...BrokerFields\n    }\n  }\n",
): typeof import("./graphql").BrokersDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Broker($cluster: String!, $id: Int!) {\n    broker(cluster: $cluster, id: $id) {\n      ...BrokerFields\n    }\n  }\n",
): typeof import("./graphql").BrokerDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n",
): typeof import("./graphql").BrokerConfigsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Topics($cluster: String!) {\n    clusterCatalog(cluster: $cluster) {\n      updatedAt\n      topics {\n        ...TopicListFields\n      }\n    }\n  }\n",
): typeof import("./graphql").TopicsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicFields\n    }\n  }\n",
): typeof import("./graphql").TopicDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n",
): typeof import("./graphql").TopicConfigsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query ConsumerGroups($cluster: String!, $topic: String) {\n    consumerGroups(cluster: $cluster, topic: $topic) {\n      ...ConsumerGroupFields\n    }\n  }\n",
): typeof import("./graphql").ConsumerGroupsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query GroupsCatalog($cluster: String!) {\n    clusterCatalog(cluster: $cluster) {\n      updatedAt\n      consumerGroups {\n        ...GroupListFields\n      }\n    }\n  }\n",
): typeof import("./graphql").GroupsCatalogDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query ConsumerGroup($cluster: String!, $id: String!) {\n    consumerGroup(cluster: $cluster, id: $id) {\n      ...ConsumerGroupFields\n    }\n  }\n",
): typeof import("./graphql").ConsumerGroupDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query TopicThroughput($cluster: String!, $topic: String!) {\n    topicThroughput(cluster: $cluster, topic: $topic) {\n      ...ThroughputPointFields\n    }\n  }\n",
): typeof import("./graphql").TopicThroughputDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query GroupLagHistory($cluster: String!, $id: String!) {\n    groupLagHistory(cluster: $cluster, id: $id) {\n      ...ThroughputPointFields\n    }\n  }\n",
): typeof import("./graphql").GroupLagHistoryDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query SchemaSubjects($cluster: String!) {\n    schemaSubjects(cluster: $cluster) {\n      ...SchemaSubjectFields\n    }\n  }\n",
): typeof import("./graphql").SchemaSubjectsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Records($query: RecordQuery!) {\n    records(query: $query) {\n      records {\n        ...TopicRecordFields\n      }\n      hasMore\n      nextCursor\n    }\n  }\n",
): typeof import("./graphql").RecordsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      hits {\n        ...SearchResultFields\n      }\n      schemaRegistryError\n    }\n  }\n",
): typeof import("./graphql").SearchDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  subscription TopicRates($cluster: String!) {\n    topicRates(cluster: $cluster) {\n      ...TopicRateFields\n    }\n  }\n",
): typeof import("./graphql").TopicRatesDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  subscription ConsumerGroupLag($cluster: String!, $id: String!) {\n    consumerGroupLag(cluster: $cluster, id: $id) {\n      ...ConsumerGroupLagFields\n    }\n  }\n",
): typeof import("./graphql").ConsumerGroupLagDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  subscription CatalogUpdated($cluster: String!) {\n    catalogUpdated(cluster: $cluster) {\n      cluster\n      updatedAt\n      generation\n    }\n  }\n",
): typeof import("./graphql").CatalogUpdatedDocument;

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}
