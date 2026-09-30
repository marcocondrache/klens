export type {
  Acl,
  BrokerRow,
  CleanupPolicy,
  ClusterHealth,
  ConfigEntry,
  GroupDetail,
  GroupMember,
  GroupOffset,
  GroupRow,
  GroupState,
  Identity,
  LaneHealth,
  LogDir,
  PartitionRow,
  PrivilegeName,
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
