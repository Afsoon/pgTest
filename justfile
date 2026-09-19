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
build:
    cargo build --workspace --all-targets

# Run cargo clean on the workspace members
clean:
    cargo clean

# Run workspace tests, including deterministic simulations (requires Docker)
test:
    cargo test --workspace

mod core "crates/pgtest-core"
mod server "crates/pgtest-server"
mod wire "crates/pgtest-wire"
mod database "crates/pgtest-database-operations"
mod utils "crates/pgtest-utils"
