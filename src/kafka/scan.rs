//! Scan-session records engine (architecture v2 phase 2).
//!
//! One consumer per page request, reused across filter passes. Two-stage
//! filtering decodes only heap candidates. Partial pages survive the deadline.

mod filter;
mod page;
mod payload;
mod session;

pub(crate) use page::fetch_page;
pub(crate) use payload::DecodedPayload;
pub use session::ScanSession;
pub(crate) use session::{RawRecord, ScanConsumer, decoded_from_bytes};
