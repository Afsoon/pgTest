WIP

## Development commands

Run `just help` to list workspace commands and all five crate modules: `core`,
`wire`, `server`, `database`, and `utils`. Each module provides `check`, `build`,
`test`, `lint`, `fmt`, `fmt-check`, and `doc` (which opens the generated docs).
`check` includes test targets; `lint` runs Clippy with warnings treated as errors.
Use `fmt-check` separately to check formatting without modifying files.

```sh
just check
just fmt-check
just database::test
just utils::test
just core::test-unit
just core::test-integration
just check-profile
just server::run-profile
```

Core unit tests run without Docker; its integration command runs the
PostgreSQL-backed manager tests. Full core, wire, and database test suites require
Docker for Testcontainers. Simulations always use their own sequential database
names; production and PostgreSQL-backed tests use random names. The obsolete
`stable_ids` Cargo feature has been removed.
`check-profile` enables `hotpath` and is also available in every crate module
except `utils`. Server startup uses an externally managed PostgreSQL instance;
there are no Compose-backed `db-*` commands.

The [v0.1 release plan](#v01-release-plan) covers independent database supply,
architecture stabilization, a native server CLI, and distroless distribution.

Connect test teardown to the virtual `pgtest` database on the proxy port (`6432`)
and release the test's lease explicitly:

```sql
SELECT pgtest_release('test-42');
-- Prepared statements also support SELECT pgtest_release($1::text).
```

Application connections still use `template/test-42`. Closing the last application
connection keeps its database assigned. Release returns `true` after the engine
closes the lease, cancels its sessions, and retains background cleanup work; it
does not wait for physical DROP. Repeated release succeeds, including release
before the first attach. Released IDs cannot reconnect for the life of this proxy
process, so use a fresh ID for each test execution.

Lease IDs contain 1–256 UTF-8 bytes and cannot contain `/` or NUL. Set
`PGTEST_MAX_LEASE_RECORDS` to bound admitted IDs (default `100000`, including
pending, active, and closed IDs). At capacity, new IDs fail; existing leases can
still be released. The existing `PGTEST_LEASE_CLAIM_TIMEOUT_MS` lifetime limit
still applies. A release acknowledgement times out after five seconds; an error
or lost response can be retried with the same ID.

Feature designs:

- [Explicit lease release through a virtual control database](docs/explicit-lease-release.md).
- [Preparing upstream sessions ahead of demand](docs/upstream-session-preparation.md).

Measured results: [Explicit lease release removes burst stalls](docs/explicit-release-performance.md).

Implementation walkthrough: [Worker engine retirement retries](docs/worker-engine-retirement-retries.md).

Database creation and cleanup run in separate workers with independent PostgreSQL
connection pools. `PGTEST_CREATION_POOL_CONNECTION` controls creation connections
(default `10`), and `PGTEST_CLEANUP_POOL_CONNECTION` controls cleanup connections
(default `5`). Both must be greater than zero. These settings limit connection
use; they do not cap queued jobs or database counts.

The engine replenishes ready database supply independently of cleanup.
Retired databases remain recorded until cleanup reports success. If the existing
PostgreSQL retry attempts fail, the retirement record remains; scheduling further
cleanup attempts is still pending in this refactor.

Hotpath profiling is opt-in for the server and its core/wire libraries:

```sh
cargo run -p pgtest-server --features hotpath
cargo run -p pgtest-server --features 'hotpath,hotpath-alloc'
```

Use the usual PostgreSQL configuration (`PGTEST_PG_HOST`, `PGTEST_PG_PORT`,
`PGTEST_PG_USER`, `PGTEST_PG_DATABASE`). Stop the server with Ctrl-C to print the
report. The default report automatically includes each instrumented section with
data; no report configuration is needed.

`PGTEST_PG_HOST` and `PGTEST_PG_PORT` select the upstream endpoint for both the
database pools and leased sessions. The defaults are `127.0.0.1` and `5432`.
Set `PGTEST_PG_HOST=postgres` to use a Docker service hostname, or, on Unix,
`PGTEST_PG_HOST=/var/run/postgresql` to use a socket directory. The socket path is
`<directory>/.s.PGSQL.<PGTEST_PG_PORT>`; provide the directory, not the socket file.
The socket must be accessible to the pgtest process. Unix upstream endpoints are
unsupported on non-Unix platforms; no TCP fallback is attempted.

Initial coverage includes database operations and SQLx queries, worker startup
and lease transitions, connection handling and parsing, relay socket I/O, the
worker inbox and lease reply channels, and Tokio runtime metrics. Socket byte
counts cover the relay after connection negotiation. Channel and I/O instances
aggregate by call site. Console log filtering does not suppress SQL profiling.

`PostgresManager::acquire_create_connection` and `acquire_drop_connection` measure
pool acquisition separately from CREATE/DROP execution. Acquisition includes any
queue wait, connection opening, and validation performed by SQLx. Acquisition
errors retain the same retry handling as query errors. Set `HOTPATH_LIMIT=0` to
show all measured functions, including these acquisition timings.

`PostgresUpstream::connect` includes three separately measured stages:
`connect_stream` opens the configured transport (`connect_tcp` also measures TCP
connection setup and sets TCP_NODELAY); `authenticate` sends
Startup and waits for AuthenticationOk; `wait_for_ready_for_query` consumes the
remaining startup messages through ReadyForQuery (or ErrorResponse). Bytes already
read during authentication remain buffered for the final stage. Use
`HOTPATH_LIMIT=0` to include all three stages even when their totals are small.

Within `pgtest-wire`, `wire_listener` owns the accept loops, `connection` handles
startup and routing, and `control_panel::serve` processes control connections.
`postgres_upstream` owns the upstream startup exchange, while `session_relay`
forwards bytes for the lifetime of an attached lease. Connection handling and
control-session profiling now appear under `connection::handle_connection` and
`control_panel::serve`.

The internal MPSC endpoint types use `hotpath::wrap`; these resolve to the
original Tokio types when profiling is disabled. Oneshot replies use proxy mode.
The regular, non-optional hotpath dependency is safe to keep: without the
`hotpath` feature, instrumentation macros are noops, with no runtime overhead or
additional profiling dependencies compiled beyond the hotpath crates themselves.
The Tokio/tracing integrations reuse dependencies already present in this project.
No profiling feature is enabled by default. `hotpath-prometheus` is also available
as a feature passthrough for use alongside `hotpath`.

For machine-readable reports:

```sh
HOTPATH_OUTPUT_FORMAT=json cargo run -p pgtest-server --features hotpath
```

For the live TUI, install `cargo install hotpath --features tui`, then run
`hotpath console` while the profiled server is running. The metrics server uses
port 6770 by default. See the [hotpath documentation](https://hotpath.rs) for details.

Verify both build modes with `cargo check` and `cargo check --features hotpath`.

## Local clients over a Unix socket

On macOS and Linux, set `PGTEST_UNIX_SOCKET_DIR` to an existing directory to add
a Unix listener alongside TCP `127.0.0.1:6432`. Both listeners support application
leases and the virtual `pgtest` control database:

```sh
mkdir -p /tmp/pgtest
PGTEST_UNIX_SOCKET_DIR=/tmp/pgtest cargo run -p pgtest-server
```

For the metered setup, `just server::run-metered-unix` creates the directory and
uses the same upstream and pool defaults as `run-metered`. Its final optional
argument is `socket_dir` (default `/tmp/pgtest`); the preceding arguments remain
`host user template port pool initial maximum`. To also enable burst summaries:

```sh
just server::run-metered-unix
```

The socket is `/tmp/pgtest/.s.PGSQL.6432`, following PostgreSQL's
[socket naming convention](https://www.postgresql.org/docs/18/runtime-config-connection.html#GUC-UNIX-SOCKET-DIRECTORIES).
Use a directory accessible only to the intended local clients. The directory
must be accessible in the same operating-system environment as the proxy;
a Docker Desktop VM cannot connect to a macOS host socket through a port mapping.

With node-postgres, pass the **directory** as `host`, and keep `port: 6432`:

```ts
import { Pool } from "pg";

const connection = { host: "/tmp/pgtest", port: 6432, user: "metered", ssl: false };
const applicationPool = new Pool({
  ...connection,
  database: `metered/${leaseId}`,
  max: 2,
});
const controlPool = new Pool({ ...connection, database: "pgtest" });

// After the test's work and all of its other application pools have stopped:
await applicationPool.end();
await controlPool.query("SELECT pgtest_release($1::text)", [leaseId]);
// Keep the shared control pool until the test runner finishes.
```

See node-postgres's [Unix socket configuration](https://node-postgres.com/features/connecting#unix-domain-sockets).
Unix connections use plaintext PostgreSQL startup; configure clients with SSL
disabled. They replace the test-to-proxy TCP hop. The upstream transport is chosen
independently through `PGTEST_PG_HOST`; it can use TCP or a Unix socket and performs
the existing startup exchange. The reported `PostgresUpstream::connect_stream`
timing measures that upstream hop, not the frontend handshake eliminated here.

The server removes its socket on Ctrl-C. Binding fails if the path already exists;
it never removes an existing socket or regular file to make room. After abnormal
termination, choose a fresh directory or confirm the old server has stopped
before removing its stale socket. Library users keep the handle returned by
`WireListener::run_unix` alive and call `shutdown().await` for completed cleanup.

## Historical attachment-window metrics

The current implementation does not read `PGTEST_PERF_WINDOWS` or emit the window
events described below. This section documents earlier measurements. For current
profiling, use `just server::run-profile`.

Each occupied window produces `attachment_window`, `creation_window`, and
`cleanup_window` events
on the `pgtest::performance` tracing target. Match their `window_start_ms` fields
to compare demand, clone supply, and latency over time. To show just those events,
use `RUST_LOG=pgtest::performance=info`.

| Fields                                                             | Measurement                                                                                                                                                           |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tcp_accepted`, `unix_accepted`                                    | Frontend connections accepted in this window, including control connections.                                                                                          |
| `attach_started`                                                   | Calls entering the manager's attachment path; includes connections joining an existing lease.                                                                         |
| `attach_ok`, `attach_failed`, `attach_cancelled`                   | Attachment outcomes; failed includes returned timeouts, cancelled means the future was dropped.                                                                       |
| `attach_avg_ms`, `attach_max_ms`                                   | Manager attachment duration for calls returning success or error. Excludes upstream connection setup and cancelled calls.                                             |
| `inbox_count`, `inbox_avg_ms`, `inbox_max_ms`                      | Time from the manager's request timestamp until the engine begins handling it.                                                                                        |
| `queued`                                                           | Attachment requests that found no ready clone and entered the waiting queue.                                                                                          |
| `ready_wait_count`, `ready_wait_avg_ms`, `ready_wait_max_ms`       | Time from entering that queue until attempting an attachment reply after a clone becomes available. Excludes expired or released waiters that never reach that reply. |
| `ready_min`, `ready_last`                                          | Minimum and latest observed ready-clone counts; `None` before the engine's first snapshot.                                                                            |
| `pending_peak`, `waiting_peak`                                     | Maximum observed pending creation/replacement reservations and distinct lease IDs waiting for a clone. Pending includes cleanup-admission and retry waits.            |
| `capacity_last`, `maximum_last`                                    | Latest allocated slot capacity and configured maximum; `None` before the engine's first snapshot. Capacity includes ready, leased, and pending slots.                 |
| `create_started`, `create_ok`, `create_failed`, `create_cancelled` | Individual CREATE attempts, including initial clones and retries.                                                                                                     |
| `create_avg_ms`, `create_max_ms`                                   | CREATE attempt duration including acquisition of a SQLx management connection. Successes and returned errors are included; cancelled attempts are excluded.           |
| `create_in_flight_last`, `create_in_flight_peak`                   | Latest and maximum number of unfinished CREATE attempts, including management connection acquisition. Excludes cleanup admission and time between attempts.           |
| `acquire_count`, `acquire_avg_ms`, `acquire_max_ms`                | Management connection acquisition for completed CREATE attempts, including failed acquisitions.                                                                       |
| `sql_count`, `sql_avg_ms`, `sql_max_ms`                            | CREATE execution after acquiring a management connection, including SQL errors.                                                                                       |
| `replacement_cleanup_last`, `replacement_cleanup_peak`             | Replacement jobs awaiting cleanup admission for their old database, before calling CREATE.                                                                            |
| `replacement_creating_last`, `replacement_creating_peak`           | Replacement jobs inside the creation call, including management acquisition and the PostgreSQL client's internal retries. Excludes initial clones and pool growth.    |
| `replacement_retrying_last`, `replacement_retrying_peak`           | Replacement jobs sleeping between failed creation calls.                                                                                                              |

The `cleanup_window` fields expose the work that can delay replacement creation:

| Fields                                                             | Measurement                                                                                                                                                                                               |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `admission_started`, `admission_ok`, `admission_cancelled`         | Requests for cleanup backlog capacity, successful admissions, and requests cancelled before admission (including shutdown).                                                                               |
| `admission_avg_ms`, `admission_max_ms`                             | Time until backlog capacity is acquired, for successful admissions. This precedes the replacement's CREATE timer and does not wait for that database's DROP to finish.                                    |
| `admission_waiting_last`, `admission_waiting_peak`                 | Requests still awaiting cleanup backlog capacity.                                                                                                                                                         |
| `cleanup_queued_last`, `cleanup_queued_peak`                       | Admitted deletion jobs awaiting their task's first poll or a cleanup execution permit.                                                                                                                    |
| `cleanup_running_last`, `cleanup_running_peak`                     | Jobs holding an execution permit while calling `drop_database`, including management acquisition and the client's internal retries.                                                                       |
| `cleanup_retrying_last`, `cleanup_retrying_peak`                   | Jobs sleeping after a failed `drop_database` call. They retain backlog capacity but release the execution permit.                                                                                         |
| `queue_wait_count`, `queue_wait_avg_ms`, `queue_wait_max_ms`       | Time from entering the admitted queue until acquiring an execution permit. Includes task scheduling on the first attempt and a new sample for each retry; excludes outer retry sleep and cancelled waits. |
| `drop_started`, `drop_ok`, `drop_failed`, `drop_cancelled`         | Individual DROP attempts, including startup sweep deletions and internal retries.                                                                                                                         |
| `drop_avg_ms`, `drop_max_ms`                                       | Completed DROP attempt duration including management connection acquisition. Excludes admission, execution-permit waits, and time between attempts.                                                       |
| `drop_acquire_count`, `drop_acquire_avg_ms`, `drop_acquire_max_ms` | Management connection acquisition for completed DROP attempts, including failed acquisitions.                                                                                                             |
| `drop_sql_count`, `drop_sql_avg_ms`, `drop_sql_max_ms`             | DROP execution after acquiring a management connection, including SQL errors.                                                                                                                             |

Stage gauges carry their current values into the next occupied window and are
released on completion or cancellation. Within each job, only one stage is
active. A replacement and its admitted cleanup are separate jobs and can run
concurrently. Startup sweep deletions contribute DROP timings without occupying
the background cleanup queue. Engine pending reservations additionally include
work not yet polled, scheduling retries, and completions awaiting engine handling;
they need not equal the sum of the replacement stages. Each peak is independent,
so adding peaks does not give a simultaneous total.

Look for these patterns across adjacent windows:

- Rising `attach_started`, `ready_min=Some(0)`, and rising `queued` and
  `ready_wait_*` indicate that demand is exhausting ready clones.
- Rising `inbox_*` while ready clones remain available points to delays before
  engine processing, rather than a wait for clone supply.
- Rising `acquire_*` means CREATE attempts spend more time obtaining management
  connections; rising `sql_*` means their execution is slower after acquisition.
  The latter alone does not identify PostgreSQL's internal cause.
- Rising `replacement_cleanup_*` and `admission_waiting_*`, followed by high
  `admission_*_ms`, identify replacement creation held behind the cleanup backlog.
  Check `cleanup_running_*`, `queue_wait_*`, and `drop_*` to separate deletion
  throughput, management connection contention, and retry delays.
- Compare `capacity_last` with `maximum_last` during shortages. If capacity is
  below its maximum while pending replacements are blocked on cleanup, the
  current growth policy may be counting that blocked work as sufficient future
  supply. Increasing the maximum alone does not change that calculation.
- Low attachment waits while first-query latency remains high directs attention
  to frontend transport, upstream startup, or the query itself. Use the existing
  upstream hotpath measurements and client-side timing to separate them.

`window_start_ms` is relative to the first recorded event. Windows are emitted
on the next activity after their boundary; empty windows are omitted. Ctrl-C
flushes the remaining window with `partial=true`. Sort by the window field when
analyzing output, since concurrent emitters can interleave log lines.

Counts of starts belong to arrival windows; durations and outcomes belong to
completion windows. A burst's completions can land in later windows. A zero
average with a zero sample count means no samples. Do not average window averages
without weighting by sample count, or add separate populations' averages/p95s.
These are aggregate correlations, not per-request traces or proof of causation.

For a TCP/Unix comparison, keep the workload, client pool sizes, and instrumentation
settings identical. Measure client connection/first-query latency and suite
duration as well: server-side acceptance starts after the frontend connection is
established, so these summaries cannot time the client's TCP handshake. Collection
uses bounded counters and a short shared lock; enabled logging has measurement
overhead, so compare both modes with the same settings.

## v0.1 release plan

The initial release target is `v0.1.0-beta.1`, using the current architecture as
the baseline after resolving the database supply/cleanup coupling. The beta is
scoped to local and CI PostgreSQL testing.

The v0.1 CLI will run the pgtest server directly as a native binary. The
distroless image will run a Linux build of the same program. PostgreSQL and the
template database remain external prerequisites. The current executable is
`pgtest-server`; the CLI interface and distribution work below are planned.

| Order | Milestone                                            | Completion criteria                                                                                                                                                          |
| ----- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1     | Separate fresh database supply from retirement       | Ready databases replenish while cleanup is delayed, subject to explicit resource limits. Cleanup-blocked replacements no longer count as immediately progressing creation.   |
| 2     | Stabilize the v0.1 architecture; refactor and review | Document ownership, lifecycle transitions, failure handling, and shutdown. Resolve correctness findings before treating the design as the v0.1 baseline.                     |
| 3     | Prepare the native CLI                               | One validated configuration path supports command-line options and the existing environment variables, with useful help, version output, startup errors, and clean shutdown. |
| 4     | Package the server in distroless                     | A reproducible Linux build runs without a shell, accepts connections through the configured interface, and handles container termination cleanly.                            |
| 5     | Validate and publish the v0.1 beta                   | Run the agreed runtime checks, record benchmark results, review release artifacts, and publish matching binary and image versions through the selected release destinations. |

### Database supply and architecture baseline

Each physical database follows `ready -> leased -> retired -> deleted`. A ready
queue supplies new leases, an active-lease map supports connections joining the
same lease, and retirement work owns deletion independently. Retired records are
removed after deletion; supplying the next database does not require reusing the
retired database's slot.

Replenishment accounts separately for ready databases, fresh creation in progress,
and retirement. Define limits for concurrent creation and outstanding databases
across all stages, including retired databases awaiting cleanup. Reaching a real
resource limit must be distinguishable from waiting because blocked retirement
work was counted as future supply. Document how the existing pool settings map to
the new policy before changing their meaning.

Preserve explicit release, process-lifetime closure of released IDs, cancellation
of lease sessions, and protection against stale background messages. Review
creation completion racing with release or shutdown, failed reply delivery,
deletion failures, and recovery of databases left by an interrupted process.

Use the new performance windows to compare repeated runs of the same burst
workload. Record ready supply, actual creations, cleanup admission waits, attachment
tail latency, and suite duration. Confirm that replenishment can progress with
cleanup waiting when the chosen resource limits still allow it.

Refactoring should make lease ownership, supply, cleanup, wire transport, and
process startup/shutdown explicit. Keep actors or storage containers as internal
implementation choices. Document the supported authentication and transport
behavior: the current upstream relay accepts trust authentication and does not
implement authentication challenges or upstream TLS. Upstream session prewarming
remains a later optimization rather than a v0.1 prerequisite.

### Native CLI

Expose upstream host, port, user and template, frontend listen address, Unix
socket directory, pool/supply limits, and optional performance windows. Use
command-line options over environment variables over defaults, preserving the
existing `PGTEST_*` configuration where its meaning is unchanged. Help and version
output must work without contacting PostgreSQL.

Keep loopback as the native default and make the frontend address and port
configurable. Validate incompatible settings before startup, report the effective
listening endpoints, and return a nonzero status on startup failure. Own both TCP
and Unix listener tasks so shutdown can stop acceptance, finish or cancel sessions
according to a documented policy, shut down the engine, remove owned socket files,
and flush the remaining metrics.

Proposed initial native release targets are macOS and Linux on arm64 and x86_64;
publish only targets exercised by the release validation. The current build uses
a floating nightly toolchain and nightly compiler options. Pin a tested toolchain
for reproducible builds, or remove those requirements after verifying a stable
build. Native binary users should not need a Rust toolchain installed.

### Distroless packaging and publication

Build in a separate stage using the pinned toolchain and lockfile. Inspect the
Linux executable's actual runtime dependencies, including native dependencies,
before choosing a maintained distroless runtime. Use a nonroot runtime and pin
the chosen base image by digest. Distroless does not provide a shell, so use an
exec-form entrypoint and avoid shell-dependent startup or health checks.
See the [official distroless documentation](https://github.com/GoogleContainerTools/distroless).

Make the container listen on an explicitly configured interface reachable through
its published port. Handle SIGTERM as well as Ctrl-C; Docker sends SIGTERM by
default when stopping a container. The current server only waits for Ctrl-C.
See [Docker's stop behavior](https://docs.docker.com/reference/cli/docker/container/stop/).
Provide a writable socket directory when Unix sockets are enabled. Validate Unix
sockets only between processes sharing the required OS/socket namespace; retain
TCP instructions for host clients across Docker Desktop's VM boundary.

Publication should include versioned native archives, checksums, a matching
container image, installation/configuration instructions, and release notes with
the supported platform and PostgreSQL compatibility matrix. Choose the binary
release destination and image registry during release preparation. A crates.io
installation route additionally requires publishable dependency metadata and an
explicit workspace package publication strategy.

Before release, validate startup, application queries, explicit release,
cancellation, shutdown, and restart cleanup using the actual native and container
artifacts. Resolve existing compiler/lint warnings and review configuration errors,
protocol error paths, task ownership, and bounded resource accounting. These are
pending release criteria; the current instrumentation and successful compile
checks alone do not establish v0.1 release readiness.
