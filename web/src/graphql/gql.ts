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
  "\n  fragment ClusterFields on Cluster {\n    name\n    label\n    clusterId\n    bootstrapServers\n    securityProtocol\n    version\n    status\n    brokerCount\n    topicCount\n    partitionCount\n    consumerGroupCount\n    underReplicatedPartitions\n    offlinePartitions\n    messageCount\n    sizeBytes\n    bytesInPerSec\n    bytesOutPerSec\n  }\n": typeof types.ClusterFieldsFragmentDoc;
  "\n  fragment BrokerFields on Broker {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n    logDirSizeBytes\n    bytesInPerSec\n    bytesOutPerSec\n  }\n": typeof types.BrokerFieldsFragmentDoc;
  "\n  fragment PartitionFields on Partition {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n    sizeBytes\n  }\n": typeof types.PartitionFieldsFragmentDoc;
  "\n  fragment TopicFields on Topic {\n    name\n    internal\n    partitions {\n      ...PartitionFields\n    }\n    replicationFactor\n    messageCount\n    sizeBytes\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    bytesInPerSec\n    messagesPerSec\n    underReplicated\n  }\n": typeof types.TopicFieldsFragmentDoc;
  "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n    documentation\n  }\n": typeof types.ConfigEntryFieldsFragmentDoc;
  "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n": typeof types.MemberAssignmentFieldsFragmentDoc;
  "\n  fragment ConsumerGroupMemberFields on ConsumerGroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n": typeof types.ConsumerGroupMemberFieldsFragmentDoc;
  "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n": typeof types.GroupOffsetFieldsFragmentDoc;
  "\n  fragment ConsumerGroupFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    members {\n      ...ConsumerGroupMemberFields\n    }\n    topics\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n": typeof types.ConsumerGroupFieldsFragmentDoc;
  "\n  fragment ThroughputPointFields on ThroughputPoint {\n    timestamp\n    bytesIn\n    bytesOut\n    messages\n  }\n": typeof types.ThroughputPointFieldsFragmentDoc;
  "\n  fragment TopicRateFields on TopicRate {\n    name\n    messagesPerSec\n    bytesInPerSec\n  }\n": typeof types.TopicRateFieldsFragmentDoc;
  "\n  fragment ConsumerGroupLagFields on ConsumerGroup {\n    id\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n": typeof types.ConsumerGroupLagFieldsFragmentDoc;
  "\n  fragment SchemaSubjectFields on SchemaSubject {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n    schema\n  }\n": typeof types.SchemaSubjectFieldsFragmentDoc;
  "\n  fragment AclFields on Acl {\n    principal\n    resourceType\n    resourceName\n    patternType\n    operation\n    permission\n    host\n  }\n": typeof types.AclFieldsFragmentDoc;
  "\n  fragment RecordHeaderFields on RecordHeader {\n    key\n    value\n  }\n": typeof types.RecordHeaderFieldsFragmentDoc;
  "\n  fragment TopicRecordFields on TopicRecord {\n    topic\n    partition\n    offset\n    timestamp\n    key\n    value\n    schemaId\n    headers {\n      ...RecordHeaderFields\n    }\n    sizeBytes\n    compression\n  }\n": typeof types.TopicRecordFieldsFragmentDoc;
  "\n  fragment SearchResultFields on SearchResult {\n    kind\n    id\n    label\n    detail\n  }\n": typeof types.SearchResultFieldsFragmentDoc;
  "\n  query Clusters {\n    clusters {\n      ...ClusterFields\n    }\n  }\n": typeof types.ClustersDocument;
  "\n  query Cluster($name: String!) {\n    cluster(name: $name) {\n      ...ClusterFields\n    }\n  }\n": typeof types.ClusterDocument;
  "\n  query Brokers($cluster: String!) {\n    brokers(cluster: $cluster) {\n      ...BrokerFields\n    }\n  }\n": typeof types.BrokersDocument;
  "\n  query Broker($cluster: String!, $id: Int!) {\n    broker(cluster: $cluster, id: $id) {\n      ...BrokerFields\n    }\n  }\n": typeof types.BrokerDocument;
  "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n": typeof types.BrokerConfigsDocument;
  "\n  query Topics($cluster: String!) {\n    topics(cluster: $cluster) {\n      ...TopicFields\n    }\n  }\n": typeof types.TopicsDocument;
  "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicFields\n    }\n  }\n": typeof types.TopicDocument;
  "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n": typeof types.TopicConfigsDocument;
  "\n  query ConsumerGroups($cluster: String!, $topic: String) {\n    consumerGroups(cluster: $cluster, topic: $topic) {\n      ...ConsumerGroupFields\n    }\n  }\n": typeof types.ConsumerGroupsDocument;
  "\n  query ConsumerGroup($cluster: String!, $id: String!) {\n    consumerGroup(cluster: $cluster, id: $id) {\n      ...ConsumerGroupFields\n    }\n  }\n": typeof types.ConsumerGroupDocument;
  "\n  query ClusterThroughput($cluster: String!) {\n    clusterThroughput(cluster: $cluster) {\n      ...ThroughputPointFields\n    }\n  }\n": typeof types.ClusterThroughputDocument;
  "\n  query TopicThroughput($cluster: String!, $topic: String!) {\n    topicThroughput(cluster: $cluster, topic: $topic) {\n      ...ThroughputPointFields\n    }\n  }\n": typeof types.TopicThroughputDocument;
  "\n  query SchemaSubjects($cluster: String!) {\n    schemaSubjects(cluster: $cluster) {\n      ...SchemaSubjectFields\n    }\n  }\n": typeof types.SchemaSubjectsDocument;
  "\n  query Acls($cluster: String!) {\n    acls(cluster: $cluster) {\n      ...AclFields\n    }\n  }\n": typeof types.AclsDocument;
  "\n  query Records($query: RecordQuery!) {\n    records(query: $query) {\n      records {\n        ...TopicRecordFields\n      }\n      hasMore\n      nextCursor\n    }\n  }\n": typeof types.RecordsDocument;
  "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      ...SearchResultFields\n    }\n  }\n": typeof types.SearchDocument;
  "\n  subscription TopicRates($cluster: String!) {\n    topicRates(cluster: $cluster) {\n      ...TopicRateFields\n    }\n  }\n": typeof types.TopicRatesDocument;
  "\n  subscription ConsumerGroupLag($cluster: String!, $id: String!) {\n    consumerGroupLag(cluster: $cluster, id: $id) {\n      ...ConsumerGroupLagFields\n    }\n  }\n": typeof types.ConsumerGroupLagDocument;
};
const documents: Documents = {
  "\n  fragment ClusterFields on Cluster {\n    name\n    label\n    clusterId\n    bootstrapServers\n    securityProtocol\n    version\n    status\n    brokerCount\n    topicCount\n    partitionCount\n    consumerGroupCount\n    underReplicatedPartitions\n    offlinePartitions\n    messageCount\n    sizeBytes\n    bytesInPerSec\n    bytesOutPerSec\n  }\n":
    types.ClusterFieldsFragmentDoc,
  "\n  fragment BrokerFields on Broker {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n    logDirSizeBytes\n    bytesInPerSec\n    bytesOutPerSec\n  }\n":
    types.BrokerFieldsFragmentDoc,
  "\n  fragment PartitionFields on Partition {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n    sizeBytes\n  }\n":
    types.PartitionFieldsFragmentDoc,
  "\n  fragment TopicFields on Topic {\n    name\n    internal\n    partitions {\n      ...PartitionFields\n    }\n    replicationFactor\n    messageCount\n    sizeBytes\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    bytesInPerSec\n    messagesPerSec\n    underReplicated\n  }\n":
    types.TopicFieldsFragmentDoc,
  "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n    documentation\n  }\n":
    types.ConfigEntryFieldsFragmentDoc,
  "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n":
    types.MemberAssignmentFieldsFragmentDoc,
  "\n  fragment ConsumerGroupMemberFields on ConsumerGroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n":
    types.ConsumerGroupMemberFieldsFragmentDoc,
  "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n":
    types.GroupOffsetFieldsFragmentDoc,
  "\n  fragment ConsumerGroupFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    members {\n      ...ConsumerGroupMemberFields\n    }\n    topics\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n":
    types.ConsumerGroupFieldsFragmentDoc,
  "\n  fragment ThroughputPointFields on ThroughputPoint {\n    timestamp\n    bytesIn\n    bytesOut\n    messages\n  }\n":
    types.ThroughputPointFieldsFragmentDoc,
  "\n  fragment TopicRateFields on TopicRate {\n    name\n    messagesPerSec\n    bytesInPerSec\n  }\n":
    types.TopicRateFieldsFragmentDoc,
  "\n  fragment ConsumerGroupLagFields on ConsumerGroup {\n    id\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n":
    types.ConsumerGroupLagFieldsFragmentDoc,
  "\n  fragment SchemaSubjectFields on SchemaSubject {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n    schema\n  }\n":
    types.SchemaSubjectFieldsFragmentDoc,
  "\n  fragment AclFields on Acl {\n    principal\n    resourceType\n    resourceName\n    patternType\n    operation\n    permission\n    host\n  }\n":
    types.AclFieldsFragmentDoc,
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
  "\n  query Brokers($cluster: String!) {\n    brokers(cluster: $cluster) {\n      ...BrokerFields\n    }\n  }\n":
    types.BrokersDocument,
  "\n  query Broker($cluster: String!, $id: Int!) {\n    broker(cluster: $cluster, id: $id) {\n      ...BrokerFields\n    }\n  }\n":
    types.BrokerDocument,
  "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n":
    types.BrokerConfigsDocument,
  "\n  query Topics($cluster: String!) {\n    topics(cluster: $cluster) {\n      ...TopicFields\n    }\n  }\n":
    types.TopicsDocument,
  "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicFields\n    }\n  }\n":
    types.TopicDocument,
  "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n":
    types.TopicConfigsDocument,
  "\n  query ConsumerGroups($cluster: String!, $topic: String) {\n    consumerGroups(cluster: $cluster, topic: $topic) {\n      ...ConsumerGroupFields\n    }\n  }\n":
    types.ConsumerGroupsDocument,
  "\n  query ConsumerGroup($cluster: String!, $id: String!) {\n    consumerGroup(cluster: $cluster, id: $id) {\n      ...ConsumerGroupFields\n    }\n  }\n":
    types.ConsumerGroupDocument,
  "\n  query ClusterThroughput($cluster: String!) {\n    clusterThroughput(cluster: $cluster) {\n      ...ThroughputPointFields\n    }\n  }\n":
    types.ClusterThroughputDocument,
  "\n  query TopicThroughput($cluster: String!, $topic: String!) {\n    topicThroughput(cluster: $cluster, topic: $topic) {\n      ...ThroughputPointFields\n    }\n  }\n":
    types.TopicThroughputDocument,
  "\n  query SchemaSubjects($cluster: String!) {\n    schemaSubjects(cluster: $cluster) {\n      ...SchemaSubjectFields\n    }\n  }\n":
    types.SchemaSubjectsDocument,
  "\n  query Acls($cluster: String!) {\n    acls(cluster: $cluster) {\n      ...AclFields\n    }\n  }\n":
    types.AclsDocument,
  "\n  query Records($query: RecordQuery!) {\n    records(query: $query) {\n      records {\n        ...TopicRecordFields\n      }\n      hasMore\n      nextCursor\n    }\n  }\n":
    types.RecordsDocument,
  "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      ...SearchResultFields\n    }\n  }\n":
    types.SearchDocument,
  "\n  subscription TopicRates($cluster: String!) {\n    topicRates(cluster: $cluster) {\n      ...TopicRateFields\n    }\n  }\n":
    types.TopicRatesDocument,
  "\n  subscription ConsumerGroupLag($cluster: String!, $id: String!) {\n    consumerGroupLag(cluster: $cluster, id: $id) {\n      ...ConsumerGroupLagFields\n    }\n  }\n":
    types.ConsumerGroupLagDocument,
};

/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ClusterFields on Cluster {\n    name\n    label\n    clusterId\n    bootstrapServers\n    securityProtocol\n    version\n    status\n    brokerCount\n    topicCount\n    partitionCount\n    consumerGroupCount\n    underReplicatedPartitions\n    offlinePartitions\n    messageCount\n    sizeBytes\n    bytesInPerSec\n    bytesOutPerSec\n  }\n",
): typeof import("./graphql").ClusterFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment BrokerFields on Broker {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n    logDirSizeBytes\n    bytesInPerSec\n    bytesOutPerSec\n  }\n",
): typeof import("./graphql").BrokerFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment PartitionFields on Partition {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n    sizeBytes\n  }\n",
): typeof import("./graphql").PartitionFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment TopicFields on Topic {\n    name\n    internal\n    partitions {\n      ...PartitionFields\n    }\n    replicationFactor\n    messageCount\n    sizeBytes\n    cleanupPolicy\n    retentionMs\n    consumerGroups\n    bytesInPerSec\n    messagesPerSec\n    underReplicated\n  }\n",
): typeof import("./graphql").TopicFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n    documentation\n  }\n",
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
  source: "\n  fragment ConsumerGroupFields on ConsumerGroup {\n    id\n    state\n    protocol\n    coordinator\n    members {\n      ...ConsumerGroupMemberFields\n    }\n    topics\n    lag\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n",
): typeof import("./graphql").ConsumerGroupFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment ThroughputPointFields on ThroughputPoint {\n    timestamp\n    bytesIn\n    bytesOut\n    messages\n  }\n",
): typeof import("./graphql").ThroughputPointFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  fragment TopicRateFields on TopicRate {\n    name\n    messagesPerSec\n    bytesInPerSec\n  }\n",
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
  source: "\n  fragment AclFields on Acl {\n    principal\n    resourceType\n    resourceName\n    patternType\n    operation\n    permission\n    host\n  }\n",
): typeof import("./graphql").AclFieldsFragmentDoc;
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
  source: "\n  query Topics($cluster: String!) {\n    topics(cluster: $cluster) {\n      ...TopicFields\n    }\n  }\n",
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
  source: "\n  query ConsumerGroup($cluster: String!, $id: String!) {\n    consumerGroup(cluster: $cluster, id: $id) {\n      ...ConsumerGroupFields\n    }\n  }\n",
): typeof import("./graphql").ConsumerGroupDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query ClusterThroughput($cluster: String!) {\n    clusterThroughput(cluster: $cluster) {\n      ...ThroughputPointFields\n    }\n  }\n",
): typeof import("./graphql").ClusterThroughputDocument;
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
  source: "\n  query SchemaSubjects($cluster: String!) {\n    schemaSubjects(cluster: $cluster) {\n      ...SchemaSubjectFields\n    }\n  }\n",
): typeof import("./graphql").SchemaSubjectsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(
  source: "\n  query Acls($cluster: String!) {\n    acls(cluster: $cluster) {\n      ...AclFields\n    }\n  }\n",
): typeof import("./graphql").AclsDocument;
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
  source: "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      ...SearchResultFields\n    }\n  }\n",
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

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}
