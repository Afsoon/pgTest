help:
    @just --list --unsorted --list-prefix '  ' --list-heading $'pgTest Workspace\n'
    @echo ''
    @just --list --unsorted --list-prefix '    ' --list-heading $'  pgTest Core\n' --justfile crates/pgtest-core/justfile
    @echo ''
    @just --list --unsorted --list-prefix '    ' --list-heading $'  pgTest Server\n' --justfile crates/pgtest-server/justfile
    @echo ''
    @just --list --unsorted --list-prefix '    ' --list-heading $'  pgTest Wire\n' --justfile crates/pgtest-wire/justfile

# Run lints on the workspace members (cargo fmt and clippy)
lint:
    cargo +nightly fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings

# Run cargo check on the workspace members
check:
    cargo check --workspace

# Run cargo build on the workspace members
build:
    cargo build --workspace --all-targets

# Run cargo clean on the workspace members
clean:
    cargo clean

# Run cargo test on the workspace members (both id-generation modes)
test:
    cargo test --workspace
    cargo test -p pgtest-core --features stable_ids

# Start the development postgres container (waits until healthy)
db-up:
    docker compose up -d --wait

# Stop the development postgres container (data volume is kept)
db-down:
    docker compose down

# Stop the development postgres container and delete its data volume
db-reset:
    docker compose down -v
    docker compose up -d --wait

# Tail logs of the development postgres container
db-logs:
    docker compose logs -f postgres

# Open a psql shell on the development database
db-psql:
    docker compose exec postgres psql -U trust -d pgtest

mod core "crates/pgtest-core"
mod server "crates/pgtest-server"
mod wire "crates/pgtest-wire"
