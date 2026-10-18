# AGENTS.md

klens is a Kafka web UI. The backend is a Rust crate in `src/` (axum, krafka) and the frontend is a React app in `web/` (TanStack Router and Query, shadcn).

## Principles

- People read this code. Optimize for the reader. Choose clear names, short functions, and an obvious control flow over clever or compact code.
- Use blank lines to separate the logical steps inside a function, such as setup, the main work, and the result. A dense block is harder to scan than the same code split into short paragraphs.
- Performance matters. Before you finish a change, ask whether it adds allocations, clones, extra network round trips to Kafka, blocking work on the async runtime, or extra renders and refetches in the UI. If it does, justify it or find another way.
- Write idiomatic Rust and idiomatic TypeScript. Follow the conventions of the surrounding code before you introduce a new pattern.
- Keep each change scoped to its task. Do not refactor unrelated code or add a dependency unless the task needs it.

## Structure

- Give each file one clear responsibility. Split a file when it starts to hold unrelated concerns.
- Model the domain with well-defined structs and enums. Make invalid states unrepresentable where it is cheap to do so.
- Prefer methods in an `impl` block over free functions. Use a free function only when no type naturally owns the behavior.
- Lay out Rust modules as `foo.rs` next to a `foo/` directory. Do not add `mod.rs` files.

## Rust

- Borrow before you clone. Take `&str` and `&[T]` over `String` and `Vec<T>` in arguments.
- Define typed errors with `thiserror`. Use `anyhow` only at the application edge. Do not `unwrap` or `expect` outside tests unless the invariant is local and obvious.
- Keep blocking work off the async runtime.

## TypeScript

- Keep strict types. Do not use `any` or non-null assertions to silence the compiler.
- Do not edit `web/src/api/types.gen.ts` by hand. Change the Rust types and run `mise run types`.
- Build UI from the existing shadcn components in `web/src/components/ui` before you write new ones.

## Tests

- Add or update tests when you change behavior. A test exists to catch a regression. Before you write one, name the bug it would catch. If you cannot, do not write it.
- Test behavior through the public surface, such as an HTTP request against `TestApp` or a call on the owning type. Do not assert on private details that a refactor would change without changing behavior.
- Do not write tests that restate the implementation, check a constant against itself, assert that a constructor sets its fields, or only check that a function returns without panicking.
- Cover the edges where bugs live, such as empty input, limits, errors from Kafka, permission denials, and concurrent updates.
- Reuse the fixtures in `src/testing.rs`, such as `FakeCluster`, `TestApp`, and `eventually`, before you write new ones. Wait on a condition, never on a fixed sleep.
- A good test fails when the code it covers breaks. `cargo mutants` checks this. If a mutation in the covered code still leaves the test passing, the test is not doing its job.
- Prefer one test with a clear name and focused assertions over many near-duplicates.

## Comments

Write a comment only to explain a non-obvious why. Do not narrate what the code does.

## Before you finish

Run the checks that cover what you changed.

- `cargo fmt --all`
- `mise run toml:fmt`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo nextest run --workspace --exclude e2e`
- `mise run web:check`

Pull request rules live in `.claude/rules/pull-requests.md`.
