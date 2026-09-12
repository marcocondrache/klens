#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Watermarks {
    pub low: i64,
    pub high: i64,
}

impl Watermarks {
    /// Total messages ever written, ignoring retention.
    pub fn messages(&self) -> u64 {
        self.high.max(0) as u64
    }
}
