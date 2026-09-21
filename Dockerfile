# syntax=docker/dockerfile:1
FROM rust:trixie AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake mold pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY rust-toolchain.toml ./
RUN rustup set profile minimal && rustup show active-toolchain

COPY Cargo.toml Cargo.lock ./
COPY .cargo/ .cargo/
COPY crates/ crates/

ARG CARGO_FEATURES=""
ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=pgtest-target-${TARGETARCH},target=/app/target,sharing=locked \
    set -eu; \
    set --; \
    if [ -n "$CARGO_FEATURES" ]; then set -- --features "$CARGO_FEATURES"; fi; \
    cargo build --locked --release -p pgtest-server --bin pgtest-server "$@"; \
    readelf -p .comment target/release/pgtest-server | grep mold; \
    install -D -m 0755 target/release/pgtest-server /out/pgtest-server

FROM gcr.io/distroless/cc-debian13
COPY --from=build /out/pgtest-server /usr/local/bin/pgtest-server
USER 65532:65532
WORKDIR /tmp
ENV PGTEST_LISTEN_ADDR=0.0.0.0:6432
EXPOSE 6432
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/pgtest-server"]
