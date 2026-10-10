use std::cmp::Ordering;

use rmcp::model::CallToolResult;
use serde_json::Value;

use crate::app::context::{ClusterHandle, Session};
use crate::app::error::ApiError;

use super::{DEFAULT_ROWS, MAX_ROWS, RESULT_BYTES};
pub(super) fn fitted<T>(
    rows: &[T],
    narrow: &str,
    result: impl Fn(&[T], Option<String>) -> Value,
) -> CallToolResult {
    fit(rows.len(), |shown| {
        let note = (shown < rows.len()).then(|| {
            format!(
                "{} of {} left out to fit the result; {narrow}",
                rows.len() - shown,
                rows.len()
            )
        });
        CallToolResult::structured(result(&rows[..shown], note))
    })
}

pub(super) fn listed<T>(
    mut rows: Vec<T>,
    asked: Option<usize>,
    narrow: Option<&str>,
    result: impl Fn(&[T], String) -> Value,
) -> CallToolResult {
    let total = rows.len();
    rows.truncate(limit(asked));
    fit(rows.len(), |shown| {
        let showing = match narrow {
            _ if shown == total => format!("{shown} of {total}"),
            Some(narrow) if shown < rows.len() => {
                format!("{shown} of {total}, as no more fit the result; {narrow} to see others")
            }
            None if shown < rows.len() => format!("{shown} of {total}, as no more fit the result"),
            Some(narrow) => {
                format!("{shown} of {total}; {narrow}, or raise `limit`, to see others")
            }
            None => format!("{shown} of {total}; raise `limit` to see others"),
        };
        CallToolResult::structured(result(&rows[..shown], showing))
    })
}

pub(super) fn fitted_lists(
    lists: &[(&str, usize)],
    longest_inner_list: usize,
    covered: &str,
    result: impl Fn(usize, Option<String>) -> Value,
) -> CallToolResult {
    let longest = lists
        .iter()
        .map(|&(_, rows)| rows)
        .fold(longest_inner_list, usize::max);
    fit(longest, |shown| {
        let cut: Vec<String> = lists
            .iter()
            .filter(|&&(_, rows)| shown < rows)
            .map(|&(list, rows)| format!("{} of {rows} {list}", rows - shown))
            .collect();
        let note = (!cut.is_empty()).then(|| {
            format!(
                "{} left out to fit the result; {covered}",
                cut.join(" and ")
            )
        });
        CallToolResult::structured(result(shown, note))
    })
}

pub(super) fn first<T>(rows: &[T], shown: usize) -> &[T] {
    &rows[..shown.min(rows.len())]
}

pub(super) fn left_out(total: usize, most: usize) -> Option<usize> {
    (total > most).then(|| total - most)
}

pub(super) fn fit(most: usize, showing: impl Fn(usize) -> CallToolResult) -> CallToolResult {
    let full = showing(most);
    if fits(&full) {
        return full;
    }
    // Counted from 1, the partition point is the most that fits.
    let counts: Vec<usize> = (1..most).collect();
    showing(counts.partition_point(|&shown| fits(&showing(shown))))
}

pub(super) fn fits(result: &CallToolResult) -> bool {
    let bytes = serde_json::to_vec(result)
        .expect("a tool result is serializable")
        .len();
    bytes <= RESULT_BYTES
}

pub(super) fn limit(asked: Option<usize>) -> usize {
    asked.unwrap_or(DEFAULT_ROWS).clamp(1, MAX_ROWS)
}

pub(super) fn name_filter(needle: Option<&str>) -> impl Fn(&str) -> bool {
    let needle = needle.map(str::to_lowercase);
    move |name| {
        needle
            .as_deref()
            .is_none_or(|needle| name.to_lowercase().contains(needle))
    }
}

pub(super) fn one_cluster<'a>(
    session: &'a Session,
    name: Option<&'a str>,
) -> Result<ClusterHandle<'a>, ApiError> {
    if let Some(name) = name {
        return session.cluster(name);
    }
    let mut visible: Vec<ClusterHandle<'a>> = session.clusters().collect();
    if visible.len() == 1 {
        return Ok(visible.remove(0));
    }
    let names: Vec<&str> = visible.iter().map(ClusterHandle::name).collect();
    Err(ApiError::unprocessable(if names.is_empty() {
        "you can see no cluster".to_owned()
    } else {
        format!("pass `cluster` as one of {}", names.join(", "))
    }))
}

pub(super) fn largest_first_unmeasured_last<T>(
    a: Option<T>,
    b: Option<T>,
    order: impl FnOnce(&T, &T) -> Ordering,
) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => order(&b, &a),
        (a, b) => a.is_none().cmp(&b.is_none()),
    }
}
