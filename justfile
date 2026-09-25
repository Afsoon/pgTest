# List workspace and crate recipes
help:
    @just --list --list-submodules --unsorted

# Format the workspace
fmt:
    cargo +nightly fmt --all

# Check workspace formatting without changing files
fmt-check:
    cargo +nightly fmt --all --check

# Run Clippy on all workspace targets
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Run cargo check on the workspace members
check:
    cargo check --workspace --all-targets

# Check all workspace targets with profiling enabled
check-profile:
    cargo check --workspace --all-targets --features hotpath

# Run cargo build on the workspace members
build version commit_sha:
    @test -n {{quote(version)}} && test -n {{quote(commit_sha)}} || { echo 'version and commit_sha must not be empty' >&2; exit 1; }
    PGTEST_VERSION={{quote(version)}} PGTEST_COMMIT_SHA={{quote(commit_sha)}} cargo build --workspace --all-targets

# Build the local release server binary
build-release:
    cargo build --locked --release -p server --bin server

# Build the local release server binary with Hotpath
build-release-profile:
    cargo build --locked --release -p server --bin server --features hotpath

# Build the release Docker image (pgtest-server:<tag>)
docker-build tag="latest":
    docker build --build-arg CARGO_FEATURES= -t {{quote("pgtest-server:" + tag)}} .

# Build the Docker image with Hotpath (pgtest-server-hotpath:<tag>)
docker-build-profile tag="latest":
    docker build --build-arg CARGO_FEATURES=hotpath -t {{quote("pgtest-server-hotpath:" + tag)}} .

# Build three warming modes from the same release binary; pass hotpath for diagnostic images
docker-build-warm-comparison tag="step19" features="":
    #!/usr/bin/env bash
    set -euo pipefail
    base={{quote("pgtest-server:" + tag + "-base")}}
    prefix={{quote("pgtest-server:" + tag)}}
    docker build --build-arg CARGO_FEATURES={{quote(features)}} -t "$base" .
    for mode in disabled bounded background; do
        count=1
        wait_ms=5000
        if [ "$mode" = disabled ]; then count=0; fi
        if [ "$mode" = background ]; then wait_ms=0; fi
        docker build --build-arg "BASE_IMAGE=$base" \
            --build-arg "WARM_COUNT=$count" --build-arg "WARM_WAIT_MS=$wait_ms" \
            -t "$prefix-$mode" - <<'DOCKERFILE'
    ARG BASE_IMAGE
    FROM ${BASE_IMAGE}
    ARG WARM_COUNT
    ARG WARM_WAIT_MS
    ENV PGTEST_CONNECTION_WARM_COUNT=${WARM_COUNT} \
        PGTEST_CONNECTION_WARM_MAX_TOTAL=32 \
        PGTEST_CONNECTION_WARM_CONCURRENCY=4 \
        PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS=${WARM_WAIT_MS} \
        PGTEST_CONNECTION_WARM_PARAMS='{"client_encoding":"UTF8"}'
    DOCKERFILE
    done

# Run cargo clean on the workspace members
clean:
    cargo clean

# Run workspace tests, including deterministic simulations (requires Docker)
test:
    cargo nextest run --locked --workspace --no-fail-fast
    cargo test --locked --workspace --doc --no-fail-fast

mod core "crates/pgtest-core"
mod server "apps/server"
mod wire "crates/pgtest-wire"
mod database "crates/pgtest-database-operations"
mod utils "crates/pgtest-utils"
mod cli "apps/cli"
