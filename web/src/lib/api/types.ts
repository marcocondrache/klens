export type {
  Acl,
  BrokerRow,
  CleanupPolicy,
  ClientQuota,
  ClusterHealth,
  ConfigEntry,
  CreateTopic,
  GroupDetail,
  GroupMember,
  GroupOffset,
  GroupRow,
  GroupState,
  Identity,
  LaneHealth,
  LogDir,
  PartitionRow,
  PayloadEncoding,
  PrivilegeName,
  ProduceRecord,
  ProducedRecord,
  QuotaEntity,
  QuotaEntityType,
  Record as KafkaRecord,
  RecordOrder,
  SubjectRow,
  TopicDetail,
  TopicGroupRow,
  TopicRow,
} from "@/api/types.gen";

import type { SearchHit as SearchHitWire } from "@/api/types.gen";

export type SearchHit = SearchHitWire & {
  href: string;
};
