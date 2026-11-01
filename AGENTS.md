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

## Comments

Write a comment only to explain a non-obvious why. Do not narrate what the code does.

## Checks

Run the checks that cover what you changed. In Claude Code, a Stop hook runs them and a PostToolUse hook formats each edited file.

- `cargo fmt --all`
- `mise run toml:fmt`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo nextest run --workspace --exclude e2e`
- `mise run web:check`

## More

- Rust, web, and test conventions live in `.claude/rules/`. Claude Code loads each one when it touches a matching file.
- Pull request rules live in the `pull-request` skill at `.claude/skills/pull-request/SKILL.md`.
