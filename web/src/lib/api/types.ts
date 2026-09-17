export type {
  AclFieldsFragment as Acl,
  AclAuthorizer,
  AclResourceType,
  BrokerRowFieldsFragment as BrokerRow,
  CleanupPolicy,
  ClusterHealthFieldsFragment as ClusterHealth,
  ConfigEntryFieldsFragment as ConfigEntry,
  GroupDetailFieldsFragment as GroupDetail,
  GroupMemberFieldsFragment as GroupMember,
  GroupOffsetFieldsFragment as GroupOffset,
  GroupRowFieldsFragment as GroupRow,
  GroupState,
  IdentityFieldsFragment as Identity,
  LaneHealthFieldsFragment as LaneHealth,
  PartitionRowFieldsFragment as PartitionRow,
  PointFieldsFragment as Point,
  PrivilegeName,
  RecordFieldsFragment as KafkaRecord,
  RecordOrder,
  RecordQueryInput,
  Role,
  SubjectDetailFieldsFragment as SubjectDetail,
  SubjectRowFieldsFragment as SubjectRow,
  TopicDetailFieldsFragment as TopicDetail,
  TopicGroupRowFieldsFragment as TopicGroupRow,
  TopicRowFieldsFragment as TopicRow,
  UpdateScope,
} from "@/graphql/graphql";

import type { SearchHitFieldsFragment } from "@/graphql/graphql";

export type SearchHit = SearchHitFieldsFragment & {
  href: string;
};
