//! Flat re-export of Kafka domain types for the rest of the crate.
//!
//! The `kafka` module root re-exports the product subset. This barrel also
//! has raw snapshots ([`GroupSnapshot`], [`MetadataSnapshot`]) and
//! [`FetchPlan`], which `session` and `engine` use.

pub use crate::kafka::broker::Broker;
pub use crate::kafka::cluster::{ClusterHealth, ClusterIdentity, ClusterOverview};
pub use crate::kafka::group::{
    CommittedOffset, ConsumerGroup, GroupMember, GroupOffset, GroupSnapshot, GroupState,
    MemberAssignment,
};
pub use crate::kafka::metadata::MetadataSnapshot;
pub use crate::kafka::record::plan::FetchPlan;
pub(crate) use crate::kafka::record::query::unix_datetime;
pub use crate::kafka::record::query::{RecordOrder, RecordQuery, TimestampRange};
pub use crate::kafka::record::{Compression, Record, RecordHeader, RecordPage, decode_bytes};
pub use crate::kafka::registry::{
    RegisteredSchema, SchemaCompatibility, SchemaReference, SchemaSubject, SchemaType,
};
pub use crate::kafka::search::{SearchHit, SearchKind};
pub use crate::kafka::topic::{Partition, Topic};
pub use crate::kafka::topic_config::{CleanupPolicy, ConfigEntry, ConfigSource};
pub use crate::kafka::watermarks::Watermarks;
