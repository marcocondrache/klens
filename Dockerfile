# syntax=docker/dockerfile:1

FROM oven/bun:1 AS frontend
WORKDIR /app

COPY frontend/ ./

RUN bun install --frozen-lockfile
RUN bun run build

FROM rust:1.95 AS builder
WORKDIR /app

COPY --from=frontend /app/crates/klens-server/static ./crates/klens-server/static
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release --locked --package klens \
          && cp /app/target/release/klens /app/klens

FROM cgr.dev/chainguard/glibc-dynamic@sha256:e9a3236ebb746bbab93bda4ca842e55a6aaea2c812a685646043e842e69220be

COPY --from=builder /app/klens /usr/local/bin/klens

ENTRYPOINT ["klens"]
