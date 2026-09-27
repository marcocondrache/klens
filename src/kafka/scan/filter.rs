use std::fmt::{Debug, Formatter};

use aho_corasick::AhoCorasick;

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

#[derive(Clone)]
pub struct CompiledFilter {
    needle: String,
    matcher: AhoCorasick,
}

impl PartialEq for CompiledFilter {
    fn eq(&self, other: &Self) -> bool {
        self.needle == other.needle
    }
}

impl Eq for CompiledFilter {}

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
        self.matcher.is_match(bytes)
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
    let matcher = AhoCorasick::builder()
        .ascii_case_insensitive(true)
        .build([trimmed])
        .expect("a single needle fits the automaton");
    Some(CompiledFilter {
        needle: trimmed.to_owned(),
        matcher,
    })
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
        let upper = contains("ORDER").expect("needle");
        assert!(contains("order").expect("needle").matches_bytes(b"ORDER"));
        assert!(upper.matches_bytes(b"xxorderxx"));
        assert!(upper.matches_bytes(b"xxOrDeR"));
        assert!(!upper.matches_bytes(b"ord"));
        assert!(!upper.matches_bytes(b""));
    }

    #[test]
    fn case_folding_is_ascii_only() {
        let filter = contains("ÉTAT").expect("needle");
        assert!(filter.matches_bytes("l'ÉTAT".as_bytes()));
        assert!(!filter.matches_bytes("l'état".as_bytes()));
    }
}
