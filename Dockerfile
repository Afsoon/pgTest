# syntax=docker/dockerfile:1
FROM rust:trixie AS chef

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake mold pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY rust-toolchain.toml ./
RUN rustup set profile minimal && rustup show active-toolchain
RUN cargo install cargo-chef --locked --version 0.1.78
COPY .cargo/ .cargo/

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY apps/ apps/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /app/recipe.json recipe.json
ARG CARGO_FEATURES=""
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    set -eu; \
    set --; \
    if [ -n "$CARGO_FEATURES" ]; then set -- --features "$CARGO_FEATURES"; fi; \
    cargo chef cook --locked --release -p server --bin server --recipe-path recipe.json "$@"

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY apps/ apps/
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    set -eu; \
    set --; \
    if [ -n "$CARGO_FEATURES" ]; then set -- --features "$CARGO_FEATURES"; fi; \
    cargo build --locked --release -p server --bin server "$@"; \
    readelf -p .comment target/release/server | grep mold; \
    install -D -m 0755 target/release/server /out/pgtest-server

FROM gcr.io/distroless/cc-debian13
COPY --from=build /out/pgtest-server /usr/local/bin/pgtest-server
USER 65532:65532
WORKDIR /tmp
ENV PGTEST_LISTEN_ADDR=0.0.0.0
ENV PGTEST_LISTEN_PORT=6432
EXPOSE 6432
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/pgtest-server"]
