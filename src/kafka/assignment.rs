use crate::kafka::model::MemberAssignment;

/// Decode a standard consumer-protocol assignment blob.
pub fn parse_consumer_assignment(bytes: &[u8]) -> Vec<MemberAssignment> {
    let mut cursor = Cursor::new(bytes);

    let Some(_version) = cursor.i16() else {
        return Vec::new();
    };

    let Some(topic_count) = cursor.i32() else {
        return Vec::new();
    };
    if topic_count < 0 {
        return Vec::new();
    }

    let mut assignments = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let Some(topic) = cursor.string() else {
            return assignments;
        };
        let Some(partition_count) = cursor.i32() else {
            return assignments;
        };
        if partition_count < 0 {
            return assignments;
        }

        let mut partitions = Vec::with_capacity(partition_count as usize);
        for _ in 0..partition_count {
            let Some(partition) = cursor.i32() else {
                return assignments;
            };
            partitions.push(partition);
        }

        assignments.push(MemberAssignment { topic, partitions });
    }

    assignments
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.offset.checked_add(n)?;
        let slice = self.bytes.get(self.offset..end)?;
        self.offset = end;
        Some(slice)
    }

    fn i16(&mut self) -> Option<i16> {
        let bytes = self.take(2)?;
        Some(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn i32(&mut self) -> Option<i32> {
        let bytes = self.take(4)?;
        Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn string(&mut self) -> Option<String> {
        let len = self.i16()?;
        if len < 0 {
            return None;
        }
        let bytes = self.take(len as usize)?;
        Some(String::from_utf8_lossy(bytes).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(topics: &[(&str, &[i32])]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&(topics.len() as i32).to_be_bytes());

        for (name, partitions) in topics {
            out.extend_from_slice(&(name.len() as i16).to_be_bytes());
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
            for partition in *partitions {
                out.extend_from_slice(&partition.to_be_bytes());
            }
        }

        out
    }

    #[test]
    fn parses_version_zero_assignment() {
        let bytes = encode(&[("orders.created", &[0, 2]), ("payments.captured", &[1])]);
        let assignments = parse_consumer_assignment(&bytes);

        assert_eq!(
            assignments,
            vec![
                MemberAssignment {
                    topic: "orders.created".into(),
                    partitions: vec![0, 2],
                },
                MemberAssignment {
                    topic: "payments.captured".into(),
                    partitions: vec![1],
                },
            ]
        );
    }

    #[test]
    fn empty_and_truncated_blobs_are_safe() {
        assert!(parse_consumer_assignment(&[]).is_empty());
        assert!(parse_consumer_assignment(&[0, 0, 0]).is_empty());
    }
}
