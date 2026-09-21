# Rust example with the native CLI

A small users repository using Tokio and `tokio-postgres`. Testcontainers runs
PostgreSQL; PgTest runs as a native child process built from this checkout.

## Run

Use Linux or macOS with Rust nightly installed through rustup, cargo-nextest, and
a running Docker daemon.
Linux GNU builds also require GCC and mold, as configured in the repository.
From the repository root:

```sh
cd example/example-rust-cli
rustup run nightly cargo nextest run --locked
```

The test builds the CLI automatically with Cargo and reads the executable path
from Cargo's build output. The first build may take several minutes. To skip it,
point `PGTEST_BIN` to a prebuilt executable:

```sh
PGTEST_BIN=../../target/release/pgtest rustup run nightly cargo nextest run --locked
```

No PgTest Docker image is needed. Testcontainers pulls `postgres:18-alpine` as
needed. Both PostgreSQL and the CLI use dynamically assigned ports.

This example is a separate Cargo workspace, so root workspace test commands do
not run it. `just test` in this directory runs the same command above.

## What it demonstrates

PostgreSQL initializes a template containing a `users` table and one seed user.
It uses the same PostgreSQL startup options as the JavaScript example, including
`file_copy_method=clone`, with trust authentication for PgTest's connections.

One Tokio integration test owns one PostgreSQL container and one PgTest process.
It runs two user scenarios concurrently: each inserts user `1` with a different
name and sees only its own data plus the seed user. Another scenario shows that
two connections sharing a lease see the same committed data.

The `with_lease` helper assigns a unique ID, connects to
`test_template/<lease-id>`, closes application connections, and releases the lease
through the `pgtest` control database. It also checks that released IDs reject new
connections. Cleanup runs on returned errors and assertion failures. The suite
stops the CLI with SIGTERM and removes the PostgreSQL container afterward.

Leases last up to two minutes; the scenarios have a separate 30-second timeout.
See [the test](tests/users.rs), [setup and cleanup](tests/support/mod.rs),
[template SQL](tests/schema.sql), and [repository functions](src/lib.rs).
