export type {
  Acl,
  BrokerRow,
  CleanupPolicy,
  ClientQuota,
  ClusterHealth,
  ConfigEntry,
  GroupDetail,
  GroupMember,
  GroupOffset,
  GroupRow,
  GroupState,
  HangingPartition,
  HangingReason,
  Identity,
  LaneHealth,
  LogDir,
  OpenTransaction,
  PartitionRow,
  PrivilegeName,
  QuotaEntity,
  QuotaEntityType,
  Record as KafkaRecord,
  RecordOrder,
  SubjectRow,
  TopicDetail,
  TopicGroupRow,
  TopicPartition,
  TopicRow,
  TransactionCoverage,
  Transactions,
  TransactionState,
} from "@/api/types.gen";

import type { SearchHit as SearchHitWire } from "@/api/types.gen";

export type SearchHit = SearchHitWire & {
  href: string;
};
