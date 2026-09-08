# syntax=docker/dockerfile:1

FROM oven/bun:1 AS web
WORKDIR /app

COPY Cargo.toml schema.graphql ./
COPY web/ ./web/

WORKDIR /app/web
RUN bun install --frozen-lockfile
RUN bun run build && test -f /app/static/index.html

FROM rust:1.95 AS builder
WORKDIR /app

# Compile crates.io dependencies in their own layer so GitHub Actions
# `cache-to: type=gha` can reuse them. That backend stores image layers
# only; BuildKit cache mounts (for example /app/target) are not exported
# and are empty on ephemeral runners, which made `cargo build`
# recompile everything whenever COPY . . invalidated the following RUN.
COPY Cargo.toml Cargo.lock ./
COPY xtask/Cargo.toml xtask/Cargo.toml
RUN mkdir -p src xtask/src static \
    && echo 'fn main() {}' > src/main.rs \
    && echo '// placeholder' > src/lib.rs \
    && echo 'fn main() {}' > xtask/src/main.rs \
    && echo '<!doctype html><title></title>' > static/index.html

RUN cargo build --release --locked --features ui --package klens \
    && rm -rf src \
        target/release/klens \
        target/release/libklens* \
        target/release/deps/klens-* \
        target/release/deps/libklens-* \
        target/release/.fingerprint/klens-*

COPY --from=web /app/static ./static
COPY src ./src

RUN cargo build --release --locked --features ui --package klens \
    && cp /app/target/release/klens /app/klens

FROM cgr.dev/chainguard/glibc-dynamic@sha256:e9a3236ebb746bbab93bda4ca842e55a6aaea2c812a685646043e842e69220be

WORKDIR /
COPY --from=builder /app/klens /usr/local/bin/klens

ENTRYPOINT ["klens"]
