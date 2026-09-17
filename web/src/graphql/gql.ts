/* eslint-disable */
import * as types from './graphql';



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
    "\n  fragment IdentityFields on Identity {\n    subject\n    clusters {\n      cluster\n      roles\n      privileges\n    }\n  }\n": typeof types.IdentityFieldsFragmentDoc,
    "\n  fragment LaneHealthFields on LaneHealth {\n    updatedAt\n    checkedAt\n    lastError\n    lastPollMs\n    healthy\n  }\n": typeof types.LaneHealthFieldsFragmentDoc,
    "\n  fragment ClusterHealthFields on ClusterHealth {\n    cluster\n    ready\n    topology {\n      ...LaneHealthFields\n    }\n    watermarks {\n      ...LaneHealthFields\n    }\n    offsets {\n      ...LaneHealthFields\n    }\n    configs {\n      ...LaneHealthFields\n    }\n    subjects {\n      ...LaneHealthFields\n    }\n    topicCount\n    partitionCount\n    groupCount\n    brokerCount\n    subjectCount\n    underReplicatedPartitions\n    offlinePartitions\n  }\n": typeof types.ClusterHealthFieldsFragmentDoc,
    "\n  fragment TopicRowFields on TopicRow {\n    name\n    internal\n    partitionCount\n    replicationFactor\n    retainedMessages\n    producedTotal\n    rate\n    retentionMs\n    cleanupPolicy\n    groupCount\n    underReplicated\n  }\n": typeof types.TopicRowFieldsFragmentDoc,
    "\n  fragment PartitionRowFields on PartitionRow {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n    retained\n    underReplicated\n  }\n": typeof types.PartitionRowFieldsFragmentDoc,
    "\n  fragment TopicDetailFields on TopicDetail {\n    name\n    internal\n    replicationFactor\n    retainedMessages\n    producedTotal\n    groupCount\n    underReplicated\n    partitions {\n      ...PartitionRowFields\n    }\n  }\n": typeof types.TopicDetailFieldsFragmentDoc,
    "\n  fragment TopicGroupRowFields on TopicGroupRow {\n    id\n    state\n    memberCount\n    lagOnTopic\n  }\n": typeof types.TopicGroupRowFieldsFragmentDoc,
    "\n  fragment GroupRowFields on GroupRow {\n    id\n    state\n    memberCount\n    topicNames\n    totalLag\n    lagComplete\n    coordinatorId\n  }\n": typeof types.GroupRowFieldsFragmentDoc,
    "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n": typeof types.MemberAssignmentFieldsFragmentDoc,
    "\n  fragment GroupMemberFields on GroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n": typeof types.GroupMemberFieldsFragmentDoc,
    "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n": typeof types.GroupOffsetFieldsFragmentDoc,
    "\n  fragment GroupDetailFields on GroupDetail {\n    id\n    state\n    protocol\n    coordinatorId\n    totalLag\n    lagComplete\n    members {\n      ...GroupMemberFields\n    }\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n": typeof types.GroupDetailFieldsFragmentDoc,
    "\n  fragment BrokerRowFields on BrokerRow {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n  }\n": typeof types.BrokerRowFieldsFragmentDoc,
    "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n  }\n": typeof types.ConfigEntryFieldsFragmentDoc,
    "\n  fragment SubjectRowFields on SubjectRow {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n  }\n": typeof types.SubjectRowFieldsFragmentDoc,
    "\n  fragment SubjectDetailFields on SubjectDetail {\n    subject\n    version\n    id\n    type\n    schema\n    references {\n      name\n      subject\n      version\n    }\n  }\n": typeof types.SubjectDetailFieldsFragmentDoc,
    "\n  fragment AclFields on Acl {\n    resourceType\n    resourceName\n    patternType\n    principal\n    host\n    operation\n    permission\n  }\n": typeof types.AclFieldsFragmentDoc,
    "\n  fragment RecordHeaderFields on RecordHeader {\n    key\n    value\n  }\n": typeof types.RecordHeaderFieldsFragmentDoc,
    "\n  fragment RecordFields on Record {\n    topic\n    partition\n    offset\n    timestamp\n    key\n    value\n    schemaId\n    sizeBytes\n    compression\n    headers {\n      ...RecordHeaderFields\n    }\n  }\n": typeof types.RecordFieldsFragmentDoc,
    "\n  fragment PointFields on Point {\n    at\n    value\n  }\n": typeof types.PointFieldsFragmentDoc,
    "\n  fragment SearchHitFields on SearchHit {\n    kind\n    id\n    label\n    detail\n  }\n": typeof types.SearchHitFieldsFragmentDoc,
    "\n  query Whoami {\n    whoami {\n      ...IdentityFields\n    }\n  }\n": typeof types.WhoamiDocument,
    "\n  query Clusters {\n    clusters {\n      ...ClusterHealthFields\n    }\n  }\n": typeof types.ClustersDocument,
    "\n  query TopicRows($cluster: String!) {\n    topicRows(cluster: $cluster) {\n      rows {\n        ...TopicRowFields\n      }\n    }\n  }\n": typeof types.TopicRowsDocument,
    "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicDetailFields\n    }\n    topicRows(cluster: $cluster, filter: { contains: $name }) {\n      rows {\n        ...TopicRowFields\n      }\n    }\n  }\n": typeof types.TopicDocument,
    "\n  query TopicGroups($cluster: String!, $topic: String!) {\n    topicGroups(cluster: $cluster, topic: $topic) {\n      ...TopicGroupRowFields\n    }\n  }\n": typeof types.TopicGroupsDocument,
    "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n": typeof types.TopicConfigsDocument,
    "\n  query GroupRows($cluster: String!) {\n    groupRows(cluster: $cluster) {\n      rows {\n        ...GroupRowFields\n      }\n    }\n  }\n": typeof types.GroupRowsDocument,
    "\n  query Group($cluster: String!, $id: String!) {\n    group(cluster: $cluster, id: $id) {\n      ...GroupDetailFields\n    }\n  }\n": typeof types.GroupDocument,
    "\n  query BrokerRows($cluster: String!) {\n    brokerRows(cluster: $cluster) {\n      ...BrokerRowFields\n    }\n  }\n": typeof types.BrokerRowsDocument,
    "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n": typeof types.BrokerConfigsDocument,
    "\n  query SubjectRows($cluster: String!) {\n    subjectRows(cluster: $cluster) {\n      rows {\n        ...SubjectRowFields\n      }\n      sourceHealth {\n        ...LaneHealthFields\n      }\n    }\n  }\n": typeof types.SubjectRowsDocument,
    "\n  query Subject($cluster: String!, $name: String!, $version: Int) {\n    subject(cluster: $cluster, name: $name, version: $version) {\n      ...SubjectDetailFields\n    }\n  }\n": typeof types.SubjectDocument,
    "\n  query Acls($cluster: String!) {\n    acls(cluster: $cluster) {\n      authorizer\n      bindings {\n        ...AclFields\n      }\n    }\n  }\n": typeof types.AclsDocument,
    "\n  query Records($cluster: String!, $query: RecordQueryInput!) {\n    records(cluster: $cluster, query: $query) {\n      complete\n      obfuscated\n      nextCursor\n      prevCursor\n      records {\n        ...RecordFields\n      }\n    }\n  }\n": typeof types.RecordsDocument,
    "\n  query TopicRateHistory($cluster: String!, $topic: String!) {\n    topicRateHistory(cluster: $cluster, topic: $topic) {\n      ...PointFields\n    }\n  }\n": typeof types.TopicRateHistoryDocument,
    "\n  query GroupLagHistory($cluster: String!, $group: String!) {\n    groupLagHistory(cluster: $cluster, group: $group) {\n      ...PointFields\n    }\n  }\n": typeof types.GroupLagHistoryDocument,
    "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      ...SearchHitFields\n    }\n  }\n": typeof types.SearchDocument,
    "\n  subscription Updates($cluster: String!, $scope: UpdateScope) {\n    updates(cluster: $cluster, scope: $scope) {\n      __typename\n      ... on WatermarksTick {\n        at\n        clusterRate\n        topics {\n          topic\n          rate\n        }\n      }\n      ... on GroupLagUpdate {\n        at\n        group\n        lag\n        lagComplete\n        offsets {\n          ...GroupOffsetFields\n        }\n      }\n      ... on TopologyDelta {\n        version\n        addedTopics\n        removedTopics\n        changedTopics\n        addedGroups\n        removedGroups\n        changedGroups\n        brokersChanged\n      }\n      ... on ConfigsChanged {\n        version\n        configTopics: topics\n      }\n      ... on SubjectsChanged {\n        version\n        added\n        removed\n        changed\n      }\n      ... on Resync {\n        reason\n      }\n    }\n  }\n": typeof types.UpdatesDocument,
};
const documents: Documents = {
    "\n  fragment IdentityFields on Identity {\n    subject\n    clusters {\n      cluster\n      roles\n      privileges\n    }\n  }\n": types.IdentityFieldsFragmentDoc,
    "\n  fragment LaneHealthFields on LaneHealth {\n    updatedAt\n    checkedAt\n    lastError\n    lastPollMs\n    healthy\n  }\n": types.LaneHealthFieldsFragmentDoc,
    "\n  fragment ClusterHealthFields on ClusterHealth {\n    cluster\n    ready\n    topology {\n      ...LaneHealthFields\n    }\n    watermarks {\n      ...LaneHealthFields\n    }\n    offsets {\n      ...LaneHealthFields\n    }\n    configs {\n      ...LaneHealthFields\n    }\n    subjects {\n      ...LaneHealthFields\n    }\n    topicCount\n    partitionCount\n    groupCount\n    brokerCount\n    subjectCount\n    underReplicatedPartitions\n    offlinePartitions\n  }\n": types.ClusterHealthFieldsFragmentDoc,
    "\n  fragment TopicRowFields on TopicRow {\n    name\n    internal\n    partitionCount\n    replicationFactor\n    retainedMessages\n    producedTotal\n    rate\n    retentionMs\n    cleanupPolicy\n    groupCount\n    underReplicated\n  }\n": types.TopicRowFieldsFragmentDoc,
    "\n  fragment PartitionRowFields on PartitionRow {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n    retained\n    underReplicated\n  }\n": types.PartitionRowFieldsFragmentDoc,
    "\n  fragment TopicDetailFields on TopicDetail {\n    name\n    internal\n    replicationFactor\n    retainedMessages\n    producedTotal\n    groupCount\n    underReplicated\n    partitions {\n      ...PartitionRowFields\n    }\n  }\n": types.TopicDetailFieldsFragmentDoc,
    "\n  fragment TopicGroupRowFields on TopicGroupRow {\n    id\n    state\n    memberCount\n    lagOnTopic\n  }\n": types.TopicGroupRowFieldsFragmentDoc,
    "\n  fragment GroupRowFields on GroupRow {\n    id\n    state\n    memberCount\n    topicNames\n    totalLag\n    lagComplete\n    coordinatorId\n  }\n": types.GroupRowFieldsFragmentDoc,
    "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n": types.MemberAssignmentFieldsFragmentDoc,
    "\n  fragment GroupMemberFields on GroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n": types.GroupMemberFieldsFragmentDoc,
    "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n": types.GroupOffsetFieldsFragmentDoc,
    "\n  fragment GroupDetailFields on GroupDetail {\n    id\n    state\n    protocol\n    coordinatorId\n    totalLag\n    lagComplete\n    members {\n      ...GroupMemberFields\n    }\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n": types.GroupDetailFieldsFragmentDoc,
    "\n  fragment BrokerRowFields on BrokerRow {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n  }\n": types.BrokerRowFieldsFragmentDoc,
    "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n  }\n": types.ConfigEntryFieldsFragmentDoc,
    "\n  fragment SubjectRowFields on SubjectRow {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n  }\n": types.SubjectRowFieldsFragmentDoc,
    "\n  fragment SubjectDetailFields on SubjectDetail {\n    subject\n    version\n    id\n    type\n    schema\n    references {\n      name\n      subject\n      version\n    }\n  }\n": types.SubjectDetailFieldsFragmentDoc,
    "\n  fragment AclFields on Acl {\n    resourceType\n    resourceName\n    patternType\n    principal\n    host\n    operation\n    permission\n  }\n": types.AclFieldsFragmentDoc,
    "\n  fragment RecordHeaderFields on RecordHeader {\n    key\n    value\n  }\n": types.RecordHeaderFieldsFragmentDoc,
    "\n  fragment RecordFields on Record {\n    topic\n    partition\n    offset\n    timestamp\n    key\n    value\n    schemaId\n    sizeBytes\n    compression\n    headers {\n      ...RecordHeaderFields\n    }\n  }\n": types.RecordFieldsFragmentDoc,
    "\n  fragment PointFields on Point {\n    at\n    value\n  }\n": types.PointFieldsFragmentDoc,
    "\n  fragment SearchHitFields on SearchHit {\n    kind\n    id\n    label\n    detail\n  }\n": types.SearchHitFieldsFragmentDoc,
    "\n  query Whoami {\n    whoami {\n      ...IdentityFields\n    }\n  }\n": types.WhoamiDocument,
    "\n  query Clusters {\n    clusters {\n      ...ClusterHealthFields\n    }\n  }\n": types.ClustersDocument,
    "\n  query TopicRows($cluster: String!) {\n    topicRows(cluster: $cluster) {\n      rows {\n        ...TopicRowFields\n      }\n    }\n  }\n": types.TopicRowsDocument,
    "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicDetailFields\n    }\n    topicRows(cluster: $cluster, filter: { contains: $name }) {\n      rows {\n        ...TopicRowFields\n      }\n    }\n  }\n": types.TopicDocument,
    "\n  query TopicGroups($cluster: String!, $topic: String!) {\n    topicGroups(cluster: $cluster, topic: $topic) {\n      ...TopicGroupRowFields\n    }\n  }\n": types.TopicGroupsDocument,
    "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n": types.TopicConfigsDocument,
    "\n  query GroupRows($cluster: String!) {\n    groupRows(cluster: $cluster) {\n      rows {\n        ...GroupRowFields\n      }\n    }\n  }\n": types.GroupRowsDocument,
    "\n  query Group($cluster: String!, $id: String!) {\n    group(cluster: $cluster, id: $id) {\n      ...GroupDetailFields\n    }\n  }\n": types.GroupDocument,
    "\n  query BrokerRows($cluster: String!) {\n    brokerRows(cluster: $cluster) {\n      ...BrokerRowFields\n    }\n  }\n": types.BrokerRowsDocument,
    "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n": types.BrokerConfigsDocument,
    "\n  query SubjectRows($cluster: String!) {\n    subjectRows(cluster: $cluster) {\n      rows {\n        ...SubjectRowFields\n      }\n      sourceHealth {\n        ...LaneHealthFields\n      }\n    }\n  }\n": types.SubjectRowsDocument,
    "\n  query Subject($cluster: String!, $name: String!, $version: Int) {\n    subject(cluster: $cluster, name: $name, version: $version) {\n      ...SubjectDetailFields\n    }\n  }\n": types.SubjectDocument,
    "\n  query Acls($cluster: String!) {\n    acls(cluster: $cluster) {\n      authorizer\n      bindings {\n        ...AclFields\n      }\n    }\n  }\n": types.AclsDocument,
    "\n  query Records($cluster: String!, $query: RecordQueryInput!) {\n    records(cluster: $cluster, query: $query) {\n      complete\n      obfuscated\n      nextCursor\n      prevCursor\n      records {\n        ...RecordFields\n      }\n    }\n  }\n": types.RecordsDocument,
    "\n  query TopicRateHistory($cluster: String!, $topic: String!) {\n    topicRateHistory(cluster: $cluster, topic: $topic) {\n      ...PointFields\n    }\n  }\n": types.TopicRateHistoryDocument,
    "\n  query GroupLagHistory($cluster: String!, $group: String!) {\n    groupLagHistory(cluster: $cluster, group: $group) {\n      ...PointFields\n    }\n  }\n": types.GroupLagHistoryDocument,
    "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      ...SearchHitFields\n    }\n  }\n": types.SearchDocument,
    "\n  subscription Updates($cluster: String!, $scope: UpdateScope) {\n    updates(cluster: $cluster, scope: $scope) {\n      __typename\n      ... on WatermarksTick {\n        at\n        clusterRate\n        topics {\n          topic\n          rate\n        }\n      }\n      ... on GroupLagUpdate {\n        at\n        group\n        lag\n        lagComplete\n        offsets {\n          ...GroupOffsetFields\n        }\n      }\n      ... on TopologyDelta {\n        version\n        addedTopics\n        removedTopics\n        changedTopics\n        addedGroups\n        removedGroups\n        changedGroups\n        brokersChanged\n      }\n      ... on ConfigsChanged {\n        version\n        configTopics: topics\n      }\n      ... on SubjectsChanged {\n        version\n        added\n        removed\n        changed\n      }\n      ... on Resync {\n        reason\n      }\n    }\n  }\n": types.UpdatesDocument,
};

/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment IdentityFields on Identity {\n    subject\n    clusters {\n      cluster\n      roles\n      privileges\n    }\n  }\n"): typeof import('./graphql').IdentityFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment LaneHealthFields on LaneHealth {\n    updatedAt\n    checkedAt\n    lastError\n    lastPollMs\n    healthy\n  }\n"): typeof import('./graphql').LaneHealthFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment ClusterHealthFields on ClusterHealth {\n    cluster\n    ready\n    topology {\n      ...LaneHealthFields\n    }\n    watermarks {\n      ...LaneHealthFields\n    }\n    offsets {\n      ...LaneHealthFields\n    }\n    configs {\n      ...LaneHealthFields\n    }\n    subjects {\n      ...LaneHealthFields\n    }\n    topicCount\n    partitionCount\n    groupCount\n    brokerCount\n    subjectCount\n    underReplicatedPartitions\n    offlinePartitions\n  }\n"): typeof import('./graphql').ClusterHealthFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment TopicRowFields on TopicRow {\n    name\n    internal\n    partitionCount\n    replicationFactor\n    retainedMessages\n    producedTotal\n    rate\n    retentionMs\n    cleanupPolicy\n    groupCount\n    underReplicated\n  }\n"): typeof import('./graphql').TopicRowFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment PartitionRowFields on PartitionRow {\n    id\n    leader\n    replicas\n    isr\n    lowWatermark\n    highWatermark\n    retained\n    underReplicated\n  }\n"): typeof import('./graphql').PartitionRowFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment TopicDetailFields on TopicDetail {\n    name\n    internal\n    replicationFactor\n    retainedMessages\n    producedTotal\n    groupCount\n    underReplicated\n    partitions {\n      ...PartitionRowFields\n    }\n  }\n"): typeof import('./graphql').TopicDetailFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment TopicGroupRowFields on TopicGroupRow {\n    id\n    state\n    memberCount\n    lagOnTopic\n  }\n"): typeof import('./graphql').TopicGroupRowFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment GroupRowFields on GroupRow {\n    id\n    state\n    memberCount\n    topicNames\n    totalLag\n    lagComplete\n    coordinatorId\n  }\n"): typeof import('./graphql').GroupRowFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment MemberAssignmentFields on MemberAssignment {\n    topic\n    partitions\n  }\n"): typeof import('./graphql').MemberAssignmentFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment GroupMemberFields on GroupMember {\n    id\n    clientId\n    host\n    assignments {\n      ...MemberAssignmentFields\n    }\n  }\n"): typeof import('./graphql').GroupMemberFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment GroupOffsetFields on GroupOffset {\n    topic\n    partition\n    currentOffset\n    endOffset\n    lag\n    memberId\n  }\n"): typeof import('./graphql').GroupOffsetFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment GroupDetailFields on GroupDetail {\n    id\n    state\n    protocol\n    coordinatorId\n    totalLag\n    lagComplete\n    members {\n      ...GroupMemberFields\n    }\n    offsets {\n      ...GroupOffsetFields\n    }\n  }\n"): typeof import('./graphql').GroupDetailFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment BrokerRowFields on BrokerRow {\n    id\n    host\n    port\n    rack\n    controller\n    partitionCount\n    leaderCount\n  }\n"): typeof import('./graphql').BrokerRowFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment ConfigEntryFields on ConfigEntry {\n    name\n    value\n    source\n    readOnly\n    sensitive\n  }\n"): typeof import('./graphql').ConfigEntryFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment SubjectRowFields on SubjectRow {\n    subject\n    id\n    type\n    latestVersion\n    versions\n    compatibility\n  }\n"): typeof import('./graphql').SubjectRowFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment SubjectDetailFields on SubjectDetail {\n    subject\n    version\n    id\n    type\n    schema\n    references {\n      name\n      subject\n      version\n    }\n  }\n"): typeof import('./graphql').SubjectDetailFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment AclFields on Acl {\n    resourceType\n    resourceName\n    patternType\n    principal\n    host\n    operation\n    permission\n  }\n"): typeof import('./graphql').AclFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment RecordHeaderFields on RecordHeader {\n    key\n    value\n  }\n"): typeof import('./graphql').RecordHeaderFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment RecordFields on Record {\n    topic\n    partition\n    offset\n    timestamp\n    key\n    value\n    schemaId\n    sizeBytes\n    compression\n    headers {\n      ...RecordHeaderFields\n    }\n  }\n"): typeof import('./graphql').RecordFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment PointFields on Point {\n    at\n    value\n  }\n"): typeof import('./graphql').PointFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  fragment SearchHitFields on SearchHit {\n    kind\n    id\n    label\n    detail\n  }\n"): typeof import('./graphql').SearchHitFieldsFragmentDoc;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Whoami {\n    whoami {\n      ...IdentityFields\n    }\n  }\n"): typeof import('./graphql').WhoamiDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Clusters {\n    clusters {\n      ...ClusterHealthFields\n    }\n  }\n"): typeof import('./graphql').ClustersDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query TopicRows($cluster: String!) {\n    topicRows(cluster: $cluster) {\n      rows {\n        ...TopicRowFields\n      }\n    }\n  }\n"): typeof import('./graphql').TopicRowsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Topic($cluster: String!, $name: String!) {\n    topic(cluster: $cluster, name: $name) {\n      ...TopicDetailFields\n    }\n    topicRows(cluster: $cluster, filter: { contains: $name }) {\n      rows {\n        ...TopicRowFields\n      }\n    }\n  }\n"): typeof import('./graphql').TopicDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query TopicGroups($cluster: String!, $topic: String!) {\n    topicGroups(cluster: $cluster, topic: $topic) {\n      ...TopicGroupRowFields\n    }\n  }\n"): typeof import('./graphql').TopicGroupsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query TopicConfigs($cluster: String!, $name: String!) {\n    topicConfigs(cluster: $cluster, name: $name) {\n      ...ConfigEntryFields\n    }\n  }\n"): typeof import('./graphql').TopicConfigsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query GroupRows($cluster: String!) {\n    groupRows(cluster: $cluster) {\n      rows {\n        ...GroupRowFields\n      }\n    }\n  }\n"): typeof import('./graphql').GroupRowsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Group($cluster: String!, $id: String!) {\n    group(cluster: $cluster, id: $id) {\n      ...GroupDetailFields\n    }\n  }\n"): typeof import('./graphql').GroupDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query BrokerRows($cluster: String!) {\n    brokerRows(cluster: $cluster) {\n      ...BrokerRowFields\n    }\n  }\n"): typeof import('./graphql').BrokerRowsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query BrokerConfigs($cluster: String!, $id: Int!) {\n    brokerConfigs(cluster: $cluster, id: $id) {\n      ...ConfigEntryFields\n    }\n  }\n"): typeof import('./graphql').BrokerConfigsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query SubjectRows($cluster: String!) {\n    subjectRows(cluster: $cluster) {\n      rows {\n        ...SubjectRowFields\n      }\n      sourceHealth {\n        ...LaneHealthFields\n      }\n    }\n  }\n"): typeof import('./graphql').SubjectRowsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Subject($cluster: String!, $name: String!, $version: Int) {\n    subject(cluster: $cluster, name: $name, version: $version) {\n      ...SubjectDetailFields\n    }\n  }\n"): typeof import('./graphql').SubjectDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Acls($cluster: String!) {\n    acls(cluster: $cluster) {\n      authorizer\n      bindings {\n        ...AclFields\n      }\n    }\n  }\n"): typeof import('./graphql').AclsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Records($cluster: String!, $query: RecordQueryInput!) {\n    records(cluster: $cluster, query: $query) {\n      complete\n      obfuscated\n      nextCursor\n      prevCursor\n      records {\n        ...RecordFields\n      }\n    }\n  }\n"): typeof import('./graphql').RecordsDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query TopicRateHistory($cluster: String!, $topic: String!) {\n    topicRateHistory(cluster: $cluster, topic: $topic) {\n      ...PointFields\n    }\n  }\n"): typeof import('./graphql').TopicRateHistoryDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query GroupLagHistory($cluster: String!, $group: String!) {\n    groupLagHistory(cluster: $cluster, group: $group) {\n      ...PointFields\n    }\n  }\n"): typeof import('./graphql').GroupLagHistoryDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  query Search($cluster: String!, $term: String!) {\n    search(cluster: $cluster, term: $term) {\n      ...SearchHitFields\n    }\n  }\n"): typeof import('./graphql').SearchDocument;
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription Updates($cluster: String!, $scope: UpdateScope) {\n    updates(cluster: $cluster, scope: $scope) {\n      __typename\n      ... on WatermarksTick {\n        at\n        clusterRate\n        topics {\n          topic\n          rate\n        }\n      }\n      ... on GroupLagUpdate {\n        at\n        group\n        lag\n        lagComplete\n        offsets {\n          ...GroupOffsetFields\n        }\n      }\n      ... on TopologyDelta {\n        version\n        addedTopics\n        removedTopics\n        changedTopics\n        addedGroups\n        removedGroups\n        changedGroups\n        brokersChanged\n      }\n      ... on ConfigsChanged {\n        version\n        configTopics: topics\n      }\n      ... on SubjectsChanged {\n        version\n        added\n        removed\n        changed\n      }\n      ... on Resync {\n        reason\n      }\n    }\n  }\n"): typeof import('./graphql').UpdatesDocument;


export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}
