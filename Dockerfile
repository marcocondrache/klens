# syntax=docker/dockerfile:1

FROM oven/bun:1 AS web
WORKDIR /app

COPY Cargo.toml ./
COPY web/ ./web/

WORKDIR /app/web
RUN bun install --frozen-lockfile
RUN bun run build && test -f /app/static/index.html

FROM lukemathwalker/cargo-chef:0.1.77-rust-1.95 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --features ui --recipe-path recipe.json

COPY --from=web /app/static ./static
COPY . .
RUN cargo build --release --locked --features ui

FROM cgr.dev/chainguard/glibc-dynamic@sha256:e9a3236ebb746bbab93bda4ca842e55a6aaea2c812a685646043e842e69220be

WORKDIR /
COPY --from=builder /app/target/release/klens /usr/local/bin/klens

ENTRYPOINT ["klens"]
