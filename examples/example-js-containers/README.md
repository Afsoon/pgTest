# JavaScript / TypeScript example with Docker

A small Hono users API backed by Drizzle and `pg`, tested with Vitest and
Testcontainers. PostgreSQL and PgTest run in Docker; the API is exercised through
Hono's `app.request()` without opening an HTTP port.

## Run

Use Node.js 24+, pnpm, and a running Docker daemon. From the repository root:

```sh
cd example/example-js-containers
pnpm install --frozen-lockfile
pnpm test
```

Testcontainers builds the repository's root Dockerfile once per suite using
BuildKit and Docker's build cache. The first build can take several minutes;
suite setup allows up to ten minutes. No local Rust toolchain or separate image
build command is needed. The generated image is cleaned up after the test run.

Set `PGTEST_IMAGE` to skip the build and use a prebuilt image instead:

```sh
PGTEST_IMAGE=pgtest-server:latest pnpm test
```

Testcontainers pulls `postgres:18-alpine` as needed and assigns host ports
automatically.

```sh
pnpm typecheck
pnpm build
```

## What it demonstrates

The suite starts one PostgreSQL container, creates a template with a `users` table
and one seed user, and closes the template connection before starting PgTest.
Both containers share a private Docker network. Trust authentication is enabled
on this disposable PostgreSQL instance because PgTest's application connections
currently require it.

Each test gets a UUID lease and connects to `test_template/<lease-id>` through
PgTest. The tests demonstrate:

- Two concurrent tests inserting user `1` with different names, each seeing only
  its own data and the template's seed user.
- Separate connections sharing committed data when their lease IDs match.
- New connections being rejected after their lease is released.

The `withLease` helper closes the test pool and calls
`SELECT pgtest_release($1::text)` on the `pgtest` control database even when a test
fails. Suite teardown stops both containers and removes their network. The lease
timeout is set to two minutes; tests have a separate 15-second timeout.

Start with [the tests](tests/users.test.ts) for setup and cleanup,
[the app](src/app.ts) for the endpoints, and [the schema](src/schema.ts) for the
table definition. The standalone `pnpm dev` server requires `DATABASE_URL` pointing
to a leased database with this schema and listens on port `3000`.
