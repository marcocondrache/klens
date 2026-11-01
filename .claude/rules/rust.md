---
paths:
  - "**/*.rs"
---

# Rust

- Prefer methods in an `impl` block over free functions. Use a free function only when no type naturally owns the behavior.
- Borrow before you clone. Take `&str` and `&[T]` over `String` and `Vec<T>` in arguments.
- Define typed errors with `thiserror`. Use `anyhow` only at the application edge. Do not `unwrap` or `expect` outside tests unless the invariant is local and obvious.
