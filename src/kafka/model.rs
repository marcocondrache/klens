//! Flat re-export of Kafka domain types for the rest of the crate.
//!
//! The `kafka` module root re-exports the product subset. This barrel also
//! has the raw broker snapshots ([`GroupSnapshot`], [`MetadataSnapshot`]) and
//! the scan port types ([`ScanConsumer`], [`RawRecord`]) that `session` and
//! the ingestion lanes use.

pub use crate::kafka::acl::{
    Acl, AclListing, AclOperation, AclPatternType, AclPermission, AclResourceType,
};
pub use crate::kafka::admin::NewTopic;
pub use crate::kafka::cluster::ClusterIdentity;
pub use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupOffset, GroupSnapshot, GroupState, MemberAssignment,
};
pub use crate::kafka::metadata::{MetadataSnapshot, TopicMetadata};
pub use crate::kafka::registry::{
    RegisteredSchema, SchemaCompatibility, SchemaReference, SchemaSubject, SchemaType,
};
pub use crate::kafka::scan::plan::PartitionWindow;
pub use crate::kafka::scan::query::{RecordOrder, RecordQuery, TimestampRange};
pub use crate::kafka::scan::session::{RawRecord, ScanConsumer};
pub use crate::kafka::scan::tail::{TailConsumer, TailPosition};
pub use crate::kafka::scan::{Compression, Record, RecordHeader, RecordPage};
pub use crate::kafka::store::{SearchHit, SearchKind};
pub use crate::kafka::topic_config::{CleanupPolicy, ConfigEntry, ConfigSource};
pub use crate::kafka::watermarks::Watermarks;
