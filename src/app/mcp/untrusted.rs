use serde::Serialize;

#[cfg(test)]
mod tests;

pub(super) struct Boundary {
    pub(super) open: String,
    pub(super) close: String,
    escaped_close: String,
}

impl Boundary {
    pub(super) fn new() -> Self {
        Self::with_marker(&format!("{:016x}", rand::random::<u64>()))
    }

    pub(super) fn with_marker(marker: &str) -> Self {
        Self {
            open: format!("<data-{marker}>"),
            close: format!("</data-{marker}>"),
            escaped_close: format!("<\\/data-{marker}>"),
        }
    }

    pub(super) fn enclose(&self, text: &impl Serialize) -> String {
        let json = serde_json::to_string(text).expect("untrusted text is serializable");
        // JSON leaves these line breaks raw, and a reader that splits lines on
        // them would see the untrusted text start a line of its own.
        let line = json
            .replace(&self.close, &self.escaped_close)
            .replace('\u{85}', "\\u0085")
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029");
        format!("{}\n{line}\n{}", self.open, self.close)
    }
}

pub(super) fn clip(text: &str, budget: usize) -> (&str, bool) {
    match text.char_indices().nth(budget) {
        Some((end, _)) => (&text[..end], true),
        None => (text, false),
    }
}
