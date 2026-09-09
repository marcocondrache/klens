#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Watermarks {
    pub low: i64,
    pub high: i64,
}

impl Watermarks {
    /// Offsets still retained in the log.
    pub fn available(&self) -> i64 {
        (self.high - self.low).max(0)
    }

    /// Total messages ever written, ignoring retention.
    pub fn messages(&self) -> u64 {
        self.high.max(0) as u64
    }
}
