import { graphql } from "@/graphql/gql";

export const IdentityFields = graphql(`
  fragment IdentityFields on Identity {
    subject
    clusters {
      cluster
      roles
      privileges
    }
  }
`);

export const LaneHealthFields = graphql(`
  fragment LaneHealthFields on LaneHealth {
    updatedAt
    checkedAt
    lastError
    lastPollMs
    healthy
  }
`);

export const ClusterHealthFields = graphql(`
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
`);

export const TopicRowFields = graphql(`
  fragment TopicRowFields on TopicRow {
    name
    internal
    partitionCount
    replicationFactor
    retainedMessages
    rate
    retentionMs
    cleanupPolicy
    groupCount
    underReplicated
  }
`);

export const PartitionRowFields = graphql(`
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
`);

export const TopicDetailFields = graphql(`
  fragment TopicDetailFields on TopicDetail {
    name
    internal
    replicationFactor
    retainedMessages
    rate
    retentionMs
    cleanupPolicy
    groupCount
    underReplicated
    partitions {
      ...PartitionRowFields
    }
  }
`);

export const TopicGroupRowFields = graphql(`
  fragment TopicGroupRowFields on TopicGroupRow {
    id
    state
    memberCount
    lagOnTopic
  }
`);

export const GroupRowFields = graphql(`
  fragment GroupRowFields on GroupRow {
    id
    state
    memberCount
    topicNames
    totalLag
    lagComplete
    coordinatorId
  }
`);

export const MemberAssignmentFields = graphql(`
  fragment MemberAssignmentFields on MemberAssignment {
    topic
    partitions
  }
`);

export const GroupMemberFields = graphql(`
  fragment GroupMemberFields on GroupMember {
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

export const GroupDetailFields = graphql(`
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
`);

export const BrokerRowFields = graphql(`
  fragment BrokerRowFields on BrokerRow {
    id
    host
    port
    rack
    controller
    partitionCount
    leaderCount
  }
`);

export const ConfigEntryFields = graphql(`
  fragment ConfigEntryFields on ConfigEntry {
    name
    value
    source
    readOnly
    sensitive
  }
`);

export const SubjectRowFields = graphql(`
  fragment SubjectRowFields on SubjectRow {
    subject
    id
    type
    latestVersion
    versions
    compatibility
  }
`);

export const SubjectDetailFields = graphql(`
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
`);

export const AclFields = graphql(`
  fragment AclFields on Acl {
    resourceType
    resourceName
    patternType
    principal
    host
    operation
    permission
  }
`);

export const RecordHeaderFields = graphql(`
  fragment RecordHeaderFields on RecordHeader {
    key
    value
  }
`);

export const RecordFields = graphql(`
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
`);

export const SearchHitFields = graphql(`
  fragment SearchHitFields on SearchHit {
    kind
    id
    label
    detail
  }
`);

export const whoamiQuery = graphql(`
  query Whoami {
    whoami {
      ...IdentityFields
    }
  }
`);

export const clustersQuery = graphql(`
  query Clusters {
    clusters {
      name
      health {
        ...ClusterHealthFields
      }
    }
  }
`);

export const topicRowsQuery = graphql(`
  query TopicRows($cluster: String!) {
    cluster(name: $cluster) {
      topics {
        rows {
          ...TopicRowFields
        }
      }
    }
  }
`);

export const topicQuery = graphql(`
  query Topic($cluster: String!, $name: String!) {
    cluster(name: $cluster) {
      topic(name: $name) {
        ...TopicDetailFields
      }
    }
  }
`);

export const topicGroupsQuery = graphql(`
  query TopicGroups($cluster: String!, $topic: String!) {
    cluster(name: $cluster) {
      topicGroups(topic: $topic) {
        ...TopicGroupRowFields
      }
    }
  }
`);

export const topicConfigsQuery = graphql(`
  query TopicConfigs($cluster: String!, $name: String!) {
    cluster(name: $cluster) {
      topicConfigs(name: $name) {
        ...ConfigEntryFields
      }
    }
  }
`);

export const groupRowsQuery = graphql(`
  query GroupRows($cluster: String!) {
    cluster(name: $cluster) {
      groups {
        rows {
          ...GroupRowFields
        }
      }
    }
  }
`);

export const groupQuery = graphql(`
  query Group($cluster: String!, $id: String!) {
    cluster(name: $cluster) {
      group(id: $id) {
        ...GroupDetailFields
      }
    }
  }
`);

export const brokerRowsQuery = graphql(`
  query BrokerRows($cluster: String!) {
    cluster(name: $cluster) {
      brokers {
        ...BrokerRowFields
      }
    }
  }
`);

export const brokerConfigsQuery = graphql(`
  query BrokerConfigs($cluster: String!, $id: Int!) {
    cluster(name: $cluster) {
      brokerConfigs(id: $id) {
        ...ConfigEntryFields
      }
    }
  }
`);

export const subjectRowsQuery = graphql(`
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
`);

export const subjectQuery = graphql(`
  query Subject($cluster: String!, $name: String!, $version: Int) {
    cluster(name: $cluster) {
      subject(name: $name, version: $version) {
        ...SubjectDetailFields
      }
    }
  }
`);

export const aclsQuery = graphql(`
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
`);

export const recordsQuery = graphql(`
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
`);

export const searchQuery = graphql(`
  query Search($cluster: String!, $term: String!) {
    cluster(name: $cluster) {
      search(term: $term) {
        ...SearchHitFields
      }
    }
  }
`);

export const updatesSubscription = graphql(`
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
`);
