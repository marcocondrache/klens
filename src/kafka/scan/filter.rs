use std::fmt::{Debug, Formatter};

use crate::kafka::scan::payload::DecodedPayload;

#[derive(Debug, Clone, Copy)]
pub struct RawField<'a> {
    pub bytes: &'a [u8],
    pub framed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    NeedsPayload,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CompiledFilter {
    needle: String,
}

impl Debug for CompiledFilter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledFilter")
            .field("source", &self.needle)
            .finish_non_exhaustive()
    }
}

impl CompiledFilter {
    fn matches_bytes(&self, bytes: &[u8]) -> bool {
        contains_ascii_ci(bytes, self.needle.as_bytes())
    }

    pub fn on_raw(&self, key: Option<RawField<'_>>, value: Option<RawField<'_>>) -> Verdict {
        let mut pending = false;
        for field in [key, value].into_iter().flatten() {
            if field.framed {
                pending = true;
            } else if self.matches_bytes(field.bytes) {
                return Verdict::Pass;
            }
        }

        if pending {
            Verdict::NeedsPayload
        } else {
            Verdict::Fail
        }
    }

    pub fn on_payload(&self, key: Option<&DecodedPayload>, value: Option<&DecodedPayload>) -> bool {
        [key, value]
            .into_iter()
            .flatten()
            .any(|payload| self.matches_bytes(payload.text().as_bytes()))
    }
}

pub fn contains(needle: &str) -> Option<CompiledFilter> {
    let trimmed = needle.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(CompiledFilter {
        needle: trimmed.to_owned(),
    })
}

fn contains_ascii_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn empty_source_is_no_filter() {
        assert_eq!(contains("  "), None);
    }

    #[test]
    fn a_substring_filter_answers_from_unframed_bytes() {
        let filter = contains("FAILED").expect("needle");
        let unframed = |bytes: &'static [u8]| RawField {
            bytes,
            framed: false,
        };

        assert_eq!(
            filter.on_raw(
                Some(unframed(b"ord_1")),
                Some(unframed(br#"{"s":"failed"}"#))
            ),
            Verdict::Pass,
            "case folds on both sides"
        );
        assert_eq!(
            filter.on_raw(Some(unframed(b"ord_1")), Some(unframed(b"{}"))),
            Verdict::Fail
        );
        assert_eq!(filter.on_raw(None, None), Verdict::Fail);
    }

    #[test]
    fn a_framed_payload_defers_the_substring_match_to_the_decode() {
        let filter = contains("failed").expect("needle");
        let framed = RawField {
            bytes: b"\0\0\0\0\x07binary",
            framed: true,
        };

        assert_eq!(
            filter.on_raw(
                Some(RawField {
                    bytes: b"ord_1",
                    framed: false
                }),
                Some(framed)
            ),
            Verdict::NeedsPayload
        );
        assert_eq!(
            filter.on_raw(
                Some(RawField {
                    bytes: b"failed-key",
                    framed: false
                }),
                Some(framed)
            ),
            Verdict::Pass,
            "an unframed key that already matches short-circuits the decode"
        );
    }

    #[test]
    fn a_substring_filter_matches_decoded_text() {
        let filter = contains("FAILED").expect("needle");
        let decoded = DecodedPayload::decoded(
            Bytes::from_static(b"\0\0\0\0\x07"),
            serde_json::json!({"status": "failed"}),
        );

        assert!(filter.on_payload(None, Some(&decoded)));
        assert!(!filter.on_payload(None, None));
    }

    #[test]
    fn ascii_case_folding_handles_boundaries() {
        assert!(contains_ascii_ci(b"ORDER", b"order"));
        assert!(contains_ascii_ci(b"xxorderxx", b"ORDER"));
        assert!(!contains_ascii_ci(b"ord", b"order"));
        assert!(contains_ascii_ci(b"", b""));
    }
}
