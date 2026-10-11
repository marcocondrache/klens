//! How deeply a `.proto` source nests, read the way protox's lexer reads it.
//!
//! protox parses each `{` level recursively, and an option's text format
//! nests `{` and `<` as well, so a schema nested a few hundred levels deep
//! overflows the stack. Brackets inside comments and strings do not count:
//! a closing one there would otherwise hide a real level.

pub(super) fn depth(source: &str) -> usize {
    let mut scan = Scan {
        bytes: source.as_bytes(),
        at: 0,
    };
    let mut braces = 0usize;
    let mut angles = 0usize;
    let mut deepest = 0;

    while let Some(byte) = scan.next_code_byte() {
        match byte {
            b'{' => braces += 1,
            b'<' => angles += 1,
            b'}' => braces = braces.saturating_sub(1),
            b'>' => angles = angles.saturating_sub(1),
            _ => continue,
        }
        deepest = deepest.max(braces + angles);
    }

    deepest
}

struct Scan<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Scan<'_> {
    /// The next byte outside comments and strings.
    fn next_code_byte(&mut self) -> Option<u8> {
        loop {
            let rest = self.bytes.get(self.at..)?;
            let &byte = rest.first()?;
            if rest.starts_with(b"//") || byte == b'#' {
                self.line_comment();
            } else if rest.starts_with(b"/*") {
                self.block_comment();
            } else if byte == b'"' || byte == b'\'' {
                self.string(byte);
            } else {
                self.at += 1;
                return Some(byte);
            }
        }
    }

    fn line_comment(&mut self) {
        self.at = self.bytes[self.at..]
            .iter()
            .position(|&byte| byte == b'\n')
            .map_or(self.bytes.len(), |newline| self.at + newline + 1);
    }

    /// protox counts a nested `/*`, and at the end of the source it ends an
    /// open comment at its last `*/`.
    fn block_comment(&mut self) {
        self.at += 2;
        let mut open = 1;
        let mut last_end = None;

        while let Some(rest) = self.bytes.get(self.at..).filter(|rest| !rest.is_empty()) {
            if rest.starts_with(b"*/") {
                self.at += 2;
                open -= 1;
                if open == 0 {
                    return;
                }
                last_end = Some(self.at);
            } else if rest.starts_with(b"/*") {
                self.at += 2;
                open += 1;
            } else {
                self.at += 1;
            }
        }

        if let Some(end) = last_end {
            self.at = end;
        }
    }

    /// A string ends at its own quote or at the end of its line, even when a
    /// backslash comes before the line break.
    fn string(&mut self, quote: u8) {
        self.at += 1;
        while let Some(&byte) = self.bytes.get(self.at) {
            self.at += 1;
            match byte {
                b'\n' => return,
                b'\\' if self.bytes.get(self.at) != Some(&b'\n') => self.at += 1,
                byte if byte == quote => return,
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closing_brackets_in_comments_and_strings_hide_no_level() {
        let source = r#"
            message A { // }
              message B { /* } /* nested */ } */
                message C { # }
                  string s = 1 [default = "}\"}"];
                  string t = 2 [default = '\'}'];
                  message D {}
                }
              }
            }
        "#;

        assert_eq!(depth(source), 4);
    }

    #[test]
    fn a_string_ends_at_its_line_break() {
        let source = "message A { string s = 1 [default = \"open\\\nmessage B { message C {} } }";

        assert_eq!(depth(source), 3);
    }

    #[test]
    fn angle_brackets_nest_with_braces_in_an_option_value() {
        let source = "message A { option (x) = { a < b { c: 1 } > }; map<string, A> m = 1; }";

        assert_eq!(depth(source), 4);
    }
}
