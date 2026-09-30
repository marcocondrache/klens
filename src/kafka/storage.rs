/// One log directory on one broker, as `DescribeLogDirs` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDir {
    pub broker: i32,
    pub path: String,
    pub error: Option<String>,
    /// Size of the volume the directory lives on. Brokers before Kafka 3.3
    /// do not report it.
    pub total_bytes: Option<i64>,
    pub usable_bytes: Option<i64>,
    /// A cordoned directory takes no new partitions (KIP-1066).
    pub cordoned: bool,
    pub replicas: Vec<ReplicaLog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLog {
    pub topic: String,
    pub partition: i32,
    pub size_bytes: i64,
    /// The copy a move between log dirs is still writing. It takes disk space
    /// but is not yet the partition's log.
    pub future: bool,
}

/// Kafka reports an unknown volume size as `-1`.
pub fn volume_bytes(reported: i64) -> Option<i64> {
    (reported >= 0).then_some(reported)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_negative_volume_size_is_unknown() {
        assert_eq!(volume_bytes(-1), None);
        assert_eq!(volume_bytes(0), Some(0));
        assert_eq!(volume_bytes(4_096), Some(4_096));
    }
}
