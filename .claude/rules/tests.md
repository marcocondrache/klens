---
paths:
  - "**/tests.rs"
  - "**/tests/**"
  - "**/testing.rs"
  - "**/testing/**"
  - "e2e/**"
---

# Tests

- Add or update tests when you change behavior. A test exists to catch a regression. Before you write one, name the bug it would catch. If you cannot, do not write it.
- Test behavior through the public surface, such as an HTTP request against `TestApp` or a call on the owning type. Do not assert on private details that a refactor would change without changing behavior.
- Do not write tests that restate the implementation, check a constant against itself, assert that a constructor sets its fields, or only check that a function returns without panicking.
- Cover the edges where bugs live, such as empty input, limits, errors from Kafka, permission denials, and concurrent updates.
- Reuse the fixtures in `src/testing.rs`, such as `FakeCluster`, `TestApp`, and `eventually`, before you write new ones. Wait on a condition, never on a fixed sleep.
- A good test fails when the code it covers breaks. `cargo mutants` checks this. If a mutation in the covered code still leaves the test passing, the test is not doing its job.
- Prefer one test with a clear name and focused assertions over many near-duplicates.
