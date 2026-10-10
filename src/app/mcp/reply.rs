//! Replies that shrink until they fit the result budget.
//!
//! A tool builds its reply from owned lists and hands it to [`fit`]. When the
//! whole reply is too large, `fit` shows fewer rows of every list until it
//! fits, and says what it left out.

use rmcp::model::CallToolResult;
use serde::ser::SerializeMap as _;
use serde::{Serialize, Serializer};

use super::{DEFAULT_ROWS, MAX_ROWS, RESULT_BYTES};

/// A named list of which a reply may show only the first rows.
pub(super) struct Rows<T> {
    name: &'static str,
    all: Vec<T>,
    shown: usize,
}

impl<T> Rows<T> {
    pub fn new(name: &'static str, all: Vec<T>) -> Self {
        Self {
            name,
            shown: all.len(),
            all,
        }
    }

    pub fn len(&self) -> usize {
        self.all.len()
    }

    fn shown(&self) -> &[T] {
        &self.all[..self.shown]
    }
}

impl<T: Serialize> Serialize for Rows<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.shown().serialize(serializer)
    }
}

/// What `fit` needs of a list, whatever its rows hold.
pub(super) trait Cut {
    fn name(&self) -> &'static str;
    fn total(&self) -> usize;
    fn shown_count(&self) -> usize;
    fn show(&mut self, rows: usize);
}

impl<T> Cut for Rows<T> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn total(&self) -> usize {
        self.all.len()
    }

    fn shown_count(&self) -> usize {
        self.shown
    }

    fn show(&mut self, rows: usize) {
        self.shown = rows.min(self.all.len());
    }
}

/// A list a client pages through with `limit`. It serializes as its rows under
/// the list's name, beside a `showing` line that counts them.
pub(super) struct Page<T> {
    rows: Rows<T>,
    matching: usize,
    narrow: Option<&'static str>,
}

impl<T> Page<T> {
    /// `narrow` tells the client how to see rows the page leaves out.
    pub fn new(
        name: &'static str,
        mut rows: Vec<T>,
        limit: Option<usize>,
        narrow: Option<&'static str>,
    ) -> Self {
        let matching = rows.len();
        rows.truncate(limit.unwrap_or(DEFAULT_ROWS).clamp(1, MAX_ROWS));
        Self {
            rows: Rows::new(name, rows),
            matching,
            narrow,
        }
    }

    fn showing(&self) -> String {
        let (shown, matching) = (self.rows.shown, self.matching);
        let cut_to_fit = shown < self.rows.len();
        match (self.narrow, cut_to_fit) {
            _ if shown == matching => format!("{shown} of {matching}"),
            (Some(narrow), true) => {
                format!("{shown} of {matching}, as no more fit the result; {narrow} to see others")
            }
            (None, true) => format!("{shown} of {matching}, as no more fit the result"),
            (Some(narrow), false) => {
                format!("{shown} of {matching}; {narrow}, or raise `limit`, to see others")
            }
            (None, false) => format!("{shown} of {matching}; raise `limit` to see others"),
        }
    }
}

impl<T: Serialize> Serialize for Page<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry(self.rows.name, &self.rows)?;
        map.serialize_entry("showing", &self.showing())?;
        map.end()
    }
}

/// A reply field that holds lists `fit` may cut.
pub(super) trait Lists {
    fn push_lists<'a>(&'a mut self, lists: &mut Vec<&'a mut dyn Cut>);
}

impl<T> Lists for Rows<T> {
    fn push_lists<'a>(&'a mut self, lists: &mut Vec<&'a mut dyn Cut>) {
        lists.push(self);
    }
}

impl<T> Lists for Page<T> {
    fn push_lists<'a>(&'a mut self, lists: &mut Vec<&'a mut dyn Cut>) {
        lists.push(&mut self.rows);
    }
}

/// Implements [`Reply`] for a struct whose listed fields hold the lists to cut.
/// A trailing string says what stays whole when a list is cut. Without one,
/// the reply says so itself, as a [`Page`] does.
macro_rules! reply {
    ($reply:ty: $($field:ident),+ $(; $kept:literal)?) => {
        impl $crate::app::mcp::reply::Reply for $reply {
            fn lists(&mut self) -> Vec<&mut dyn $crate::app::mcp::reply::Cut> {
                let mut lists = Vec::new();
                $($crate::app::mcp::reply::Lists::push_lists(&mut self.$field, &mut lists);)+
                lists
            }

            fn kept_whole(&self) -> Option<&str> {
                None $(.or(Some($kept)))?
            }
        }
    };
}
pub(super) use reply;

/// A tool result that may need cutting to fit.
pub(super) trait Reply: Serialize {
    /// Every list in the reply.
    fn lists(&mut self) -> Vec<&mut dyn Cut>;

    /// What the client keeps whole when a list is cut, or `None` when the reply
    /// says so itself, as a [`Page`] does.
    fn kept_whole(&self) -> Option<&str>;

    /// Rows of lists inside the rows of `lists`, which `cap` also cuts.
    fn inner_rows(&self) -> usize {
        0
    }

    /// Cuts the lists inside the rows of `lists` to `rows`.
    fn cap(&mut self, _rows: usize) {}

    /// A note on what `cap` cut, to follow the one on `lists`.
    fn capped_note(&self, _rows: usize) -> Option<String> {
        None
    }
}

#[derive(Serialize)]
struct Fitted<'a, R> {
    #[serde(flatten)]
    reply: &'a R,
    #[serde(skip_serializing_if = "Option::is_none")]
    truncated: Option<String>,
}

pub(super) fn fit(mut reply: impl Reply) -> CallToolResult {
    let inner = reply.inner_rows();
    let most = reply
        .lists()
        .iter()
        .map(|list| list.total())
        .fold(inner, usize::max);
    search(most, |rows| render(&mut reply, rows))
}

fn render(reply: &mut impl Reply, rows: usize) -> CallToolResult {
    reply.cap(rows);
    let mut lists = reply.lists();
    let several = lists.len() > 1;
    let cut: Vec<String> = lists
        .iter_mut()
        .filter_map(|list| {
            list.show(rows);
            let left_out = list.total() - list.shown_count();
            (left_out > 0).then(|| match several {
                true => format!("{left_out} of {} {}", list.total(), list.name()),
                false => format!("{left_out} of {}", list.total()),
            })
        })
        .collect();
    let truncated = reply.kept_whole().and_then(|kept| {
        let notes = [
            (!cut.is_empty())
                .then(|| format!("{} left out to fit the result; {kept}", cut.join(" and "))),
            reply.capped_note(rows),
        ];
        let notes: Vec<String> = notes.into_iter().flatten().collect();
        (!notes.is_empty()).then(|| notes.join(". "))
    });
    CallToolResult::structured(
        serde_json::to_value(Fitted {
            reply: &*reply,
            truncated,
        })
        .expect("a tool reply is serializable"),
    )
}

/// Renders `showing` with the most rows, or else with the most rows that
/// fit, as far as one row can fit.
pub(super) fn search(
    most: usize,
    mut showing: impl FnMut(usize) -> CallToolResult,
) -> CallToolResult {
    let full = showing(most);
    if fits(&full) {
        return full;
    }
    // Counted from 1, the partition point is the most that fits.
    let counts: Vec<usize> = (1..most).collect();
    let fitting = counts.partition_point(|&rows| fits(&showing(rows)));
    showing(fitting)
}

pub(super) fn fits(result: &CallToolResult) -> bool {
    let bytes = serde_json::to_vec(result)
        .expect("a tool result is serializable")
        .len();
    bytes <= RESULT_BYTES
}

/// The first `most` rows of a list, and how many the rest holds.
pub(super) fn prefix<T>(rows: &[T], most: usize) -> (&[T], Option<usize>) {
    let kept = &rows[..most.min(rows.len())];
    (kept, (rows.len() > most).then(|| rows.len() - most))
}
