pub(crate) fn name_filter(contains: Option<&str>) -> impl Fn(&str) -> bool {
    let needle = contains
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase);
    move |name| {
        needle
            .as_deref()
            .is_none_or(|needle| name.to_lowercase().contains(needle))
    }
}

pub(crate) fn page<T>(
    rows: Vec<T>,
    after: Option<&str>,
    limit: Option<i32>,
    key: impl Fn(&T) -> &str,
) -> (Vec<T>, Option<String>) {
    let start = match after.map(str::trim).filter(|after| !after.is_empty()) {
        None => 0,
        Some(after) => rows
            .iter()
            .position(|row| key(row) == after)
            .map_or(0, |index| index + 1),
    };

    let limit = limit
        .filter(|limit| *limit > 0)
        .map_or(rows.len(), |limit| limit as usize);

    let mut rows: Vec<T> = rows.into_iter().skip(start).collect();
    let exhausted = rows.len() <= limit;
    rows.truncate(limit);

    let next_cursor = (!exhausted)
        .then(|| rows.last().map(|row| key(row).to_owned()))
        .flatten();
    (rows, next_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(rows: &[&str]) -> Vec<String> {
        rows.iter().map(|row| (*row).to_owned()).collect()
    }

    #[test]
    fn an_absent_limit_returns_every_row() {
        let (rows, cursor) = page(keys(&["a", "b", "c"]), None, None, String::as_str);

        assert_eq!(rows, keys(&["a", "b", "c"]));
        assert_eq!(cursor, None);
    }

    #[test]
    fn paging_resumes_after_the_cursor_row() {
        let rows = keys(&["a", "b", "c", "d"]);

        let (first, cursor) = page(rows.clone(), None, Some(2), String::as_str);
        assert_eq!(first, keys(&["a", "b"]));
        assert_eq!(cursor.as_deref(), Some("b"));

        let (second, cursor) = page(rows, cursor.as_deref(), Some(2), String::as_str);
        assert_eq!(second, keys(&["c", "d"]));
        assert_eq!(cursor, None, "the last page has no cursor");
    }

    #[test]
    fn a_vanished_cursor_row_restarts_rather_than_failing() {
        let (rows, _) = page(keys(&["a", "b"]), Some("gone"), Some(1), String::as_str);

        assert_eq!(rows, keys(&["a"]));
    }

    #[test]
    fn name_filters_are_case_insensitive_substrings() {
        assert!(name_filter(Some("ORDERS"))("orders.created"));
        assert!(!name_filter(Some("pay"))("orders.created"));
        assert!(name_filter(Some("   "))("anything"));
        assert!(name_filter(None)("anything"));
    }
}
