# Ready PostgreSQL connections: guided implementation

Status: design agreed; baseline test added; external Vitest suite selected for
remaining performance measurements. Pool registration, reservations, publication,
and checkout are complete. Physical identity and lifecycle hook injection are
complete, including creation and retirement notifications. Reserved warm
attempts now establish and publish sessions with shared concurrency limits,
timeouts, and cancellation. Round-robin reservation selection and per-database
retry backoff are complete. Retirement eligibility, cancellation ownership,
and scheduler wakeups are implemented. The bounded asynchronous scheduler is
complete. Client handling supports warm checkout and immediate cold fallback.
Idle health monitoring and checkout probes discard unhealthy spares. Cleanup
now waits for warm resources to drain before physical database deletion. Pool-wide
shutdown cancels admission and drains warm resources and scheduler ownership,
including databases that never received a lease.
CLI/server startup now installs the shared pool, waits for bounded initial warm-up
or starts in background-only mode, and drains warming during shutdown.
Integration validation is complete on macOS arm64 with Docker PostgreSQL 18;
the external Vitest performance comparison remains step 19. Docker comparison
variants and run instructions are prepared; measurements are pending from the user.

Current scheduling policy: `connection-warm-count` is a lifetime successful
handoff budget per physical `DatabaseId`. Successful checkouts are not replenished.
Failed attempts and unhealthy unused spares can still be replaced. This supersedes
the continuous-refill behavior recorded in the earlier implementation steps below.

The subsequent proposal for PgBouncer/PgDog-style reusable connections is in
[connection-pooling.md](connection-pooling.md). It records the physical-database
reuse boundary and the required ownership/protocol changes; it has not replaced
the runtime policy above.

## Working agreement

The user subsequently requested implementation of the scheduling policy described
below: successful checkouts consume a lifetime budget per physical database and
are not replenished. This request authorizes the assistant's implementation,
focused tests, and documentation for that change.

The user writes all implementation code and tests. The assistant maintains this
document, gives one small step at a time, and reviews the resulting diff without
editing implementation files. Resolve review findings before advancing. Split
steps further whenever a change contains independent behaviors.

Exception agreed for step 1a: the assistant writes the baseline test while the
user collects a separate Hotpath CLI profile. Subsequent implementation remains
user-written and assistant-reviewed.

The user also explicitly delegated the focused step 3b tests to the assistant;
production parser changes remain user-written.

The user also delegated the step 4b validation tests and step 5 CLI/server
configuration wiring, including its tests and documentation, to the assistant.
The focused step 6a profile-resolution tests were subsequently delegated too.
For step 6b, the user explicitly requested tests first and will implement the
methods afterward; a temporary test-compilation failure is expected.
The user also delegated the step 7a constructor tests to the assistant.
The step 7b.1 registration tests were also delegated before implementation.
The user likewise delegated step 7b.2 reservation tests before implementation.
The step 7b.3 publication tests were also delegated before implementation.
The user delegated the step 7c checkout tests and implementation guide too;
the checkout method remains user-written, with a temporary test-compilation
failure expected until it is added.

After implementing 11a, the user requested implementation of step 11. The
assistant completed 11b and added socket-path tests covering the connected
reservation, startup, and publication behavior. This exception applies to step
11; subsequent steps retain the guided implementation agreement.

The user subsequently delegated completion of 12a. The assistant finished the
registration queue updates and bounded selector, preserving the user's shared
reservation helper. New fairness coverage remains deferred to the connected
scheduler as agreed; this exception does not authorize the remaining step 12
implementation.

The user then delegated only 12b and specified `tokio-retry2` for backoff. The
assistant implemented that retry state and extended the socket-path coverage.
Steps 12c and 12d remain pending and outside this implementation request.

The user subsequently requested 12c. The assistant implemented retirement
eligibility, database/pool cancellation ownership, wakeups, and connected socket
regressions. The scheduler in 12d remains outside this request.

The user then requested 12d. The assistant implemented the bounded scheduler
and connected socket tests. Runtime installation and the later lifecycle
integration steps remain outside this request.

The user subsequently delegated step 13. The assistant integrated optional
pool checkout into client handling and added PostgreSQL integration tests.
Enabling the pool in CLI/server startup remains later integration work.

The user then delegated step 14. The assistant added idle socket monitoring,
nonblocking checkout probes, and connected health/fallback regressions.

The user subsequently delegated step 15. The assistant added the asynchronous
cleanup barrier, the pool lifecycle implementation, resource lifetime tracking,
and connected cleanup regressions. Full pool shutdown and CLI/server startup
installation remain later work.

The user then delegated step 16. The assistant implemented pool-wide shutdown,
scheduler lifetime tracking, and connected shutdown regressions. CLI/server
runtime installation remains part of the later startup integration.

The user subsequently delegated step 17. The assistant implemented initial warm-up,
an owned runtime wrapper, CLI/server and shared-listener installation, and connected
startup regressions. The broader step 18 validation and step 19 performance
comparison remain separate work.

The user then delegated step 18. The assistant added runtime race/isolation and
environment-server subprocess coverage and ran the full correctness/build checks.
This step required no production behavior changes. Performance measurements
remain outside this step.

For step 19, the user chose to run the external Vitest suite themselves with
Docker. The assistant prepares the image variants and result checklist, then
analyzes the supplied measurements. Do not mark step 19 complete before results.

For step 8 onward, the user prefers integration coverage over isolated unit
tests. Split the implementation into small parts and defer new lifecycle tests
until the structure supports testing the connected behavior. Maintain existing
tests as APIs change; do not add tests that only exercise a mock hook or an
isolated forwarding method. Use the existing manager/PostgreSQL harness and
external Vitest suite when the relevant paths are wired.

Each instruction should identify the purpose, relevant code, expected behavior,
and verification. Each intermediate change must compile and preserve behavior
when warming is disabled. Record measurement results and any agreed design
changes here; do not silently change the design during review.

## Objective and baseline

Move PostgreSQL connection startup off the client request path by preparing fresh
connections for isolated databases. Consume each connection once; never return a
used session to the pool.

The user reported old p95 values of approximately 42 ms for connection creation,
42.14 ms for startup through AuthenticationOk, and 0.01771 ms (about 18 microseconds)
for the remaining wait through ReadyForQuery. These are historical observations,
not a baseline verified against this checkout. The current upstream `connect()`
contains `authenticate()`: do not sum their percentiles or multiply p95 by the
connection count to infer concurrent test-suite time saved.

Measure client-observed connection completion, internal startup stages, startup
duration, and full workload duration separately. Account for profiler overhead
with uninstrumented timing runs.

## Current implementation

- `crates/pgtest-wire/src/connection.rs` assigns a lease, then checks an optional
  shared warm pool using the physical database identity and exact startup
  profile. A miss opens an upstream connection with the client's parameters.
- `crates/pgtest-wire/src/postgres_upstream.rs` produces an `UpstreamSession`
  containing a socket and startup response bytes. Startup now rejects
  ErrorResponse and requires an exactly sized, idle ReadyForQuery frame.
- `crates/pgtest-wire/src/session_relay.rs` forwards the startup response bytes
  and relays traffic until completion or lease cancellation.
- Core notifies the injectable lifecycle hook after accepted initial/runtime
  database creation and before retirement cleanup submission.
- Each cleanup worker job awaits that same hook's drain barrier before issuing
  PostgreSQL deletion. The warm pool implements registration, retirement, and
  draining. CLI/server startup installs it before initial creation when enabled,
  and uses the no-op hook when disabled.
- `LeaseSession` carries the physical `DatabaseId`, database name, and lease
  cancellation token.
- `WarmReservation::establish` connects using the registered name and resolved
  profile under the pool's semaphore, then publishes the completed session.
  `ConnectionWarmer` owns the scheduler and shares the pool with both listener
  types. Startup waits only for the configured initial budget, and later
  lifecycle notifications wake background replenishment.

## Agreed behavior

### Startup and checkout

Finish startup through a successful, idle ReadyForQuery before publishing a
spare. Keep the socket and its own startup response messages together, including
ParameterStatus and BackendKeyData. Never publish an upstream ErrorResponse as a
successful warm connection.

Match the client's forwarded startup parameters exactly against one configured
profile, including user, application name, encoding, and options. PgTest supplies
the physical database; replication is unsupported. Do not substitute SET or SET
ROLE for matching startup semantics.

After lease assignment, atomically consume a matching spare for that DatabaseId
and hand it to the existing relay. Each backend serves exactly one client and is
closed afterward. Never transfer it to another database or return it to the pool.

A miss, parameter mismatch, or known unhealthy spare uses the existing cold path
without waiting for warming. Each physical database has a lifetime budget equal
to `connection-warm-count`. Successful checkouts permanently consume that budget;
idle spares and in-flight attempts reserve the remaining slots. Fill eligible
slots in the background, retrying failed attempts and replacing unhealthy unused
spares, but never replenishing successful handoffs.
Monitor unused sockets for closure/errors, without a health-query round trip on
checkout. Conservatively discard a spare that sends unsolicited data, including
notices or partial error frames; no client has used that backend yet. Failures
can still race with handoff; never replay queries once client traffic has been
forwarded.

### Architecture and lifetime

Keep socket and protocol handling in pgtest-wire. Add a protocol-independent
lifecycle hook in core with a no-op default, registered before initial database
creation. Notify it after successful initial/runtime creation and before
retirement. Carry DatabaseId in LeaseSession and use it to reject stale warm-up
completions.

Share one pool across TCP and Unix listeners. Warm unassigned ready databases and
active leased databases, never the template. Do not count unused warm sockets as
client lease attachments or let warming start a lease lifetime.

Retirement immediately disables checkout and replenishment, then cancels warm-up
and closes unused sockets. The cleanup worker waits for this work to drain before
dropping the database. Existing lease cancellation closes handed-off sessions.
Keep socket I/O and asynchronous waits outside the engine message-processing loop.

Shutdown cancels and drains all pool work, including spares for never-assigned
databases. Warm-up failures do not fail database creation or block engine progress.

### Configuration

Expose equivalent CLI and server settings, retaining the existing convention
that the CLI uses arguments and the server uses PGTEST_ environment variables.

| CLI option                          | Server environment variable              | Default                                                                       |
| ----------------------------------- | ---------------------------------------- | ----------------------------------------------------------------------------- |
| `--connection-warm-count`           | `PGTEST_CONNECTION_WARM_COUNT`           | `0` disables; experiment with `1`                                             |
| `--connection-warm-max-total`       | `PGTEST_CONNECTION_WARM_MAX_TOTAL`       | `32`                                                                          |
| `--connection-warm-concurrency`     | `PGTEST_CONNECTION_WARM_CONCURRENCY`     | `4`                                                                           |
| `--connection-warm-startup-wait-ms` | `PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS` | `5000`; `0` selects background-only                                           |
| `--connection-warm-params`          | `PGTEST_CONNECTION_WARM_PARAMS`          | JSON string-to-string map, default `{}`; missing user uses configured pg-user |

Reject database and replication keys in the configured profile. Require positive
global capacity and concurrency. A per-database budget larger than available
global capacity is best-effort, constrained by that capacity. Other client
profiles connect normally.

Count idle sockets and in-flight warm attempts against the global warm cap;
active client sessions do not count against it. Use fair scheduling across
eligible databases. Bound each attempt to five seconds; back off failed attempts
from one second, doubling to a thirty-second maximum, and cancel retries at
retirement. Reset backoff after success.

Initially wait for the budgeted initial warm-up to succeed or the startup deadline
to expire before listening. Do not wait for more sockets than the global cap
allows. Log incomplete warm-up and continue with cold fallback. Runtime creation
does not wait for warm-up. Later profile background-only mode using a zero startup
wait. Warm sockets consume PostgreSQL backend slots in addition to client sessions
and management pools; document this resource cost.

## Implementation checklist

Every checkbox is an implementation-and-review cycle, not authorization for the
assistant to write the implementation. Add focused tests alongside each behavior.

- [x] Document the agreed design and working agreement.
- [x] 1a. Add a manually run measurement of repeated connections to one lease.
- [x] 1b. Record three repeated sequential-same-lease baseline runs.
- [x] 1c. Use the user's existing external Vitest suite for representative workload
      timing and a Hotpath diagnostic run; do not build additional benchmark scenarios.
- [x] 2. Distinguish lease wait, socket connection, startup/authentication,
      remaining startup, and client readiness in measurements.
- [x] 3a. Reject ErrorResponse after AuthenticationOk with StartupRejected.
- [x] 3b. Validate exact ReadyForQuery length and idle status before accepting a session.
- [x] 4a. Add the warm-pool configuration type and defaults.
- [x] 4b. Add configuration validation.
- [x] 5. Wire CLI flags, server environment settings, and help text.
- [x] 6a. Resolve the warm startup profile without mutating configuration.
- [x] 6b. Implement exact profile matching and cold-path eligibility.
- [x] 7. Add pool storage and atomic single-use checkout by database identity.
  - [x] 7a. Add pool state and an empty constructor.
  - [x] 7b. Add capacity reservation and publication.
    - [x] 7b.1. Register database identities without consuming capacity.
    - [x] 7b.2. Add owned capacity reservations and cleanup.
    - [x] 7b.3. Publish ready sessions from owned reservations.
  - [x] 7c. Add atomic single-use checkout.
- [x] 8. Add the no-op core lifecycle interface and DatabaseId in LeaseSession.
  - [x] 8a. Carry physical database identity through the attach reply and session.
  - [x] 8b. Add the optional lifecycle interface and install it before initialization.
- [x] 9. Notify successful initial and runtime database creation.
- [x] 10. Notify retirement before cleanup is submitted.
- [x] 11. Establish warm sessions with timeouts, concurrency limits, and cancellation.
  - [x] 11a. Add a cancellable, five-second warm connection attempt.
  - [x] 11b. Run reserved attempts under the pool's shared concurrency limit.
- [x] 12. Add bounded, fair replenishment and failure backoff.
  - [x] 12a. Reserve replenishment work in round-robin database order.
  - [x] 12b. Track per-database retry deadlines and capped exponential backoff.
  - [x] 12c. Add retirement eligibility, cancellation ownership, and scheduler wakeups.
  - [x] 12d. Drive attempts with one bounded asynchronous scheduler.
- [x] 13. Integrate checkout and cold fallback with client connection handling.
- [x] 14. Monitor unused-socket health and replace dead spares.
- [x] 15. Drain unused sockets and warm attempts before database deletion.
- [x] 16. Drain pool resources and background work during shutdown.
- [x] 17. Add bounded initial warm-up and background-only startup mode.
- [x] 18. Validate integration, isolation, races, failures, and both listener types.
- [ ] 19. Compare disabled, bounded-wait, and background-only performance.

## Completed step: 1a (reference)

Create `examples/example-rust-cli/tests/connection_latency.rs`, following the
existing Unix-only integration-test convention. Reuse `tests/support/mod.rs`;
no production changes or new dependencies are needed.

1. Add an ignored Tokio test named `connection_latency_baseline`, returning
   `anyhow::Result<()>`. Use an explicit ignore reason so timing is opt-in.
2. Start one PgTest with the existing helper. Reuse the error/panic-safe shutdown
   structure in `tests/users.rs`, with a 60-second timeout around the workload.
3. Use `with_lease` to obtain one assigned lease and its connection config. Its
   initial application connection remains outside the measured samples.
4. Open 100 additional sessions sequentially with `Session::connect(config)`.
   Start an Instant immediately before each call and record elapsed time
   immediately after it succeeds. Drop each session before starting the next.
   Do not put SQL, printing, or intentional sleeps inside this timed interval.
5. Sort the durations and print the sample count plus p50, p95, and p99 in
   milliseconds with fractional precision. Use nearest-rank percentiles: for
   exactly 100 samples, zero-based indices 49, 94, and 98.
6. Let `with_lease` release the lease and ensure PgTest is stopped even on failure.
   Propagate connection failures; never silently remove failed samples.

This first workload measures repeated client connection establishment after lease
assignment. It is not yet a first-connection benchmark, a concurrent benchmark,
or a measurement of total suite time. Do not assert a machine-dependent latency
threshold. Stop for review before adding those further scenarios. Run latency
comparisons separately from other profiling workloads to avoid resource contention
confounding the measurements.

Build a release CLI from the repository root:

```sh
just cli::build-release dev dev
```

Run from `examples/example-rust-cli` with Docker available:

```sh
PGTEST_BIN=../../target/release/pgtest cargo +nightly test --locked --release --test connection_latency connection_latency_baseline -- --ignored --nocapture
```

The test helper otherwise builds a debug CLI; setting PGTEST_BIN is necessary for
the release baseline. Building and container initialization stay outside the 100
connection samples. Benchmark results describe the working tree used, including
any pre-existing local changes, not necessarily the current committed revision.

## Validation and acceptance

Verify exact database/profile selection, one consumer per backend, bounded
replenishment under concurrency, and cold fallback on mismatch/miss. Cover startup
ErrorResponse, malformed/non-idle readiness, upstream death, stale completion,
retirement during warming/checkout, lease expiry, cleanup, and shutdown. Unused
connections must not prevent database deletion.

Run focused checks after each step. After integration, run workspace tests,
formatting checks, linting, and profiling-enabled compilation. The Rust example
is a separate Cargo workspace and needs its own verification.

Compare first connections across many leases, bursts within one lease, and
sustained churn. Report connection p50/p95/p99, warm hits/misses, backend counts,
PgTest startup duration, and complete test-run time including PgTest startup.
Keep external setup/build time separately identified. Repeat runs with and without
profiling. Compare disabled, bounded-wait, and background-only warming under
identical workloads.

Correctness and lifecycle tests must pass. Keep the feature opt-in until repeated
measurements show improved client latency and the user has assessed the startup,
whole-run, and backend-resource tradeoffs.

## Review and measurement log

- Design documented. Existing helpers and connection flow inspected.
- Step 1a test written by the assistant under the agreed exception. Release
  compilation, formatting, and whitespace checks passed. Running the test binary
  normally confirmed that the measurement is ignored by default.
- Verification required explicitly selecting the installed nightly compiler via
  RUSTC: the local shell otherwise selected stable rustc even under rustup run.
  Missing dependencies were downloaded with approved network access.

### First supplied measurement: 2026-09-24

The user supplied a completed 100-sample sequential-same-lease measurement:

| Client-visible metric | Milliseconds |
| --------------------- | -----------: |
| p50                   |     2.916958 |
| p95                   |     3.904250 |
| p99                   |     4.854375 |

The accompanying Hotpath 0.25.1 report was created at
2026-09-24T09:31:45Z on macos-aarch64 with rustc 1.100.0-nightly. Its relationship
to an uninstrumented run is not established; do not label the above numbers as an
uninstrumented baseline yet. The table below transcribes the reported values;
the full JSON remains in the conversation.

| Profile stage                                  | Calls | Average ms |  p95 ms |
| ---------------------------------------------- | ----: | ---------: | ------: |
| Upstream connect, including all startup stages |   101 |       2.55 |    3.20 |
| Upstream socket connection                     |   101 |    0.69912 |    1.08 |
| Startup through AuthenticationOk               |   101 |       1.77 |    2.20 |
| Remaining startup through ReadyForQuery        |   101 |    0.08038 | 0.23641 |
| Lease attach                                   |   102 |    0.07741 | 0.21568 |

Interpretation:

- This workload does not reproduce the historical 42 ms p95. It has no
  concurrent client connection burst or runtime database replenishment.
- Upstream connection setup remains the main measured connection-stage cost.
  Socket establishment and authentication account for roughly 97% of its
  average duration. Stage percentiles must not be added or subtracted to infer
  a percentile for total duration or proxy overhead.
- Socket-read polling consumed about 0.578 ms in aggregate, whereas read_until
  elapsed durations totaled about 174 ms. This points toward asynchronous waiting,
  not expensive byte parsing; the report does not separate PostgreSQL work,
  network/virtualization delay, and scheduling delay.
- handle_connection and session_relay include session lifetime. Their totals and
  percentages overlap across concurrent tasks; they are not additive startup or
  CPU costs. One long-lived application connection and one control connection
  remain open while the samples run.
- The counts agree with the helper: 100 measured sessions plus one initial
  application session give 101 upstream connections. The control connection and
  post-release rejection account for 103 frontend handlers. Only one database
  was assigned, and no runtime creation request was sent.
- Engine startup took 195.63 ms; initial creation of 16 databases took 157.33 ms.
  CLI run duration was about 522.56 ms. These are one-run measurements, not a
  complete test-run timing including external setup.
- One spare can hide startup on a hit, but the tight same-lease loop can consume
  it faster than the replacement becomes ready. Single-use warming moves work
  earlier; it does not eliminate backend creation. Keep this workload as a
  replenishment stress case and also test first connections across ready databases.

The requested repeated runs are recorded below. The user subsequently selected
the existing external Vitest suite instead of building more benchmark scenarios.

Review note: the current test no longer has its original ignore attribute. Restore
the explicit manual-run ignore annotation before treating it as a permanent
benchmark; until restored, the documented --ignored command will skip this test.
The assistant has not changed that user edit.

### Three repeated measurements: 2026-09-24

The user supplied these after the request for release runs without Hotpath. Treat
that build configuration as an assumption until confirmed; percentile output
alone does not identify the binary or its features. The first row was also sent
in an interrupted turn and is recorded only once.

| Run | Samples |   p50 ms |   p95 ms |   p99 ms |
| --- | ------: | -------: | -------: | -------: |
| 1   |     100 | 2.948917 | 4.306917 | 6.414292 |
| 2   |     100 | 2.892083 | 4.502208 | 6.273208 |
| 3   |     100 | 2.875333 | 3.949000 | 4.864750 |

The medians span 2.875333–2.948917 ms; p95 spans 3.949000–4.502208 ms.
This supports a roughly 3 ms median for this sequential same-lease workload.
The historical 42 ms p95 remains unreproduced here. These runs do not establish
a profiler-overhead estimate: variation, build configuration, and the small
sample count prevent attributing the differences to profiling. With 100 samples,
nearest-rank p99 is the second-largest observation and is sensitive to outliers.
Do not average these run percentiles into a combined percentile.

### Workload choice: existing external Vitest suite

The user selected their existing Vitest suite, which lives outside this repository
and already covers the representative workloads. Cancel the proposed
start_with_pool helper change and additional synthetic benchmark scenarios.
Retain the completed baseline test and recorded measurements as reference.

The next measurement comes from the user running that suite with the current
CLI: record its normal full-run duration and a Hotpath diagnostic report. Identify
whether the reported duration includes PgTest startup and keep instrumented and
uninstrumented timings distinct. Use the same suite settings, concurrency,
PostgreSQL configuration, and transport for before/after comparisons.

Evaluate disabled, bounded-wait, and background-only warming with this real suite
once implementation is ready. Add focused correctness tests for pool behavior as
planned, but do not duplicate the suite's performance scenarios. The user continues
writing the feature implementation, with small steps and assistant review.

### External Vitest profile: 2026-09-24T09:50:47Z

User-supplied Hotpath 0.25.1 report, rustc 1.100.0-nightly, linux-aarch64,
caller server::main. This is the server executable on Linux, distinct from the
earlier native macOS CLI benchmark. The report covers approximately 4.80 seconds
of server lifetime; it is not an uninstrumented Vitest wall-clock measurement.

| Operation                               | Calls | Average ms |  p95 ms |
| --------------------------------------- | ----: | ---------: | ------: |
| Upstream connect                        |   405 |       7.55 |   19.66 |
| Startup through AuthenticationOk        |   405 |       6.96 |   16.73 |
| Upstream socket connection              |   405 |    0.57681 |    3.20 |
| Remaining startup through ReadyForQuery |   405 |    0.01329 | 0.02459 |
| Lease attach                            |   405 |       1.14 |    4.51 |
| Create database (core wrapper)          |   272 |      37.45 |   65.21 |
| Drop database (core wrapper)            |   253 |      25.26 |   67.90 |

There were 253 database-assignment calls and 240 runtime creation requests.
Together with 272 creation calls overall, this is consistent with an initial
pool of 32. There were 420 frontend handlers: 405 leased sessions and 15 control
sessions. Roughly 1.6 upstream sessions per assignment makes preparing the first
connection per database a relevant initial experiment; counts alone do not
predict the hit rate or per-database connection distribution.

Authentication accounts for about 92% of average upstream setup duration. Sending
startup averages 7.75 microseconds and decoding authentication averages 446 ns;
these are small compared with waiting for the response. This identifies the
stage to move off the client path, not the cause inside PostgreSQL or the host.
Database creation/cleanup also overlap with connection establishment; contention
is plausible but not established by aggregate measurements.

Total upstream connect time is 3.06 seconds accumulated across concurrent calls,
not 3.06 seconds recoverable from wall-clock runtime. Likewise, long relay and
handler durations include session lifetime and cannot be read as setup latency.
Engine initialization took 543.79 ms, including 382.32 ms initial creation and
147.74 ms startup cleanup. Full-suite uninstrumented duration remains pending;
it does not block beginning the implementation.

The existing profile distinguishes the relevant startup stages sufficiently to
begin the planned correctness work. Do not build more benchmark machinery.
Continue with step 3 in small pieces, while retaining this profile for comparison.

Next user code change (step 3a): in wait_for_ready_for_query, return the existing
StartupRejected error on ErrorResponse instead of returning successful startup
bytes. Keep successful ReadyForQuery burst preservation unchanged in this step.
Split the existing burst-preservation test so it still covers the successful
ParameterStatus/ReadyForQuery burst, and add a separate AuthenticationOk followed
by ErrorResponse rejection case using connect_with_reply. The existing connection
handler maps this error to its generic connection failure, just as for rejection
before AuthenticationOk; the raw late-startup error will no longer be forwarded.
Review that change before step 3b adds exact ReadyForQuery length/idle validation.

### Step 3a review

The user's production change correctly returns StartupRejected for ErrorResponse
after authentication. Formatting and whitespace checks pass. Focused upstream
tests: five pass, one fails. startup_preserves_the_opaque_burst_after_authentication
still includes ErrorResponse as a successful burst and unwraps the error.

Keep the successful burst-preservation case and move the late ErrorResponse into
a separate rejection test. Step 3a remains pending that test update and review;
no implementation files were edited by the assistant.

Second review: the user split the tests. All seven focused upstream tests now
pass, as do formatting and whitespace checks. One assertion still needs tightening:
startup_rejected_return_error uses is_err(), which would also accept UnexpectedEof
if ErrorResponse were ignored until the mock backend closed. Assert the specific
StartupRejected variant before marking step 3a complete.

Final step 3a review: the assertion now matches StartupRejected specifically.
All seven upstream tests pass; formatting and whitespace checks pass. Step 3a is
complete. The assistant has not edited implementation files.

### Next user step: 3b

Validate the ReadyForQuery frame in wait_for_ready_for_query before returning
success. Its length field must equal 5 (four length bytes plus one status byte),
so the complete frame including the Z tag is six bytes. Reuse InvalidFrameLength
for any other length. Check length before accessing the payload.

Require status byte I. Add InvalidReadyForQueryStatus(u8) for T, E, or any unknown
status byte. This restriction is for newly established upstream sessions, not
query responses during normal relay.

Preserve the frame's starting cursor until validation completes; the buffer may
contain earlier ParameterStatus/BackendKeyData messages. Return the entire startup
burst through frame_end as before. Do not index the status relative to the start
of the whole buffer or advance cursor before retaining the current frame offset.

Use connect_with_reply for focused cases: AuthenticationOk plus Z with length 4;
length 6 with two payload bytes; valid length 5 with T, E, or an unknown byte.
Assert InvalidFrameLength or InvalidReadyForQueryStatus respectively. Retain the
existing successful ParameterStatus plus idle ReadyForQuery case. Stop for review
after this parser change and its tests; do not begin the pool yet.

### Step 3b review and tests

The assistant added four tests as requested: missing status, extra payload,
non-idle/unknown statuses with and without preceding ParameterStatus, and successful
idle readiness without preceding messages. The existing successful full-burst test
also remains in place. Formatting and whitespace checks pass.

The focused suite currently has seven passing and four failing tests. Two parser
corrections are needed in the user's implementation:

- Return bytes through frame_end, not cursor; cursor now points at the beginning
  of ReadyForQuery, so split_to(cursor) omits that message entirely.
- Reject a frame whose total length is not six with InvalidFrameLength before
  reading its payload. The current else branch indexes buf[cursor + 5] even when
  no status byte exists, causing a panic; oversized frames are also misclassified
  as invalid status. The length error should carry the declared wire length
  (frame length minus one), e.g. 4 or 6 for the new cases.

After the length guard, check the status and return InvalidReadyForQueryStatus
only for a non-I byte. Step 3b remains pending the user's fixes and another review.

Final step 3b review: length is now checked before payload access and successful
startup returns bytes through frame_end. All eleven focused upstream tests pass;
formatting and whitespace checks pass. Step 3b is complete. The optional get()
check is redundant after the exact-length guard but does not affect correctness.

### Next user step: 4a

Add a public connection_warm module in pgtest-wire, backed by
crates/pgtest-wire/src/connection_warm.rs and declared in lib.rs. Define
ConnectionWarmConfig with Clone and Debug plus an explicit Default implementation:

| Public field   | Rust type                | Default      |
| -------------- | ------------------------ | ------------ |
| per_database   | u16                      | 0            |
| max_total      | usize                    | 32           |
| concurrency    | usize                    | 4            |
| startup_wait   | Duration                 | five seconds |
| startup_params | BTreeMap<String, String> | empty        |

An empty parameter map is unresolved configuration; the configured pg-user is
supplied later when constructing the actual warm profile. Zero per_database
disables warming, and zero startup_wait selects background-only initialization.
Document these meanings and that max_total includes idle and in-flight warm
connections. Stop after the type, defaults, and module declaration for review.
Validation and CLI/server wiring are separate steps; this adds no runtime behavior.

Step 4a review: field types and defaults are correct. The wire library compiles,
and formatting/whitespace checks pass. The module declaration and fields are
still private, so CLI/server crates cannot access and configure the type. Make
the module and all five fields public and add the agreed field documentation.
Step 4a remains pending these updates; the assistant did not edit implementation.

Follow-up review: the user added a public new(...) constructor instead of exposing
the fields. Accept that as an alternative: CLI/server callers can supply all five
values while fields remain private. The connection_warm module is still private,
so it must be exported (pub mod connection_warm in lib.rs) before external crates
can reach either the type or constructor. Document the configuration meanings on
the public type/constructor. No compile rerun is needed to establish this unchanged
visibility issue; step 4a remains pending those two changes.

Final step 4a review: the user exported connection_warm publicly. At the user's
request, the assistant added Rust documentation to the configuration type, fields,
and constructor, covering defaults, zero values, capacity accounting, and later
user resolution. The constructor continues storing values without validation.
Formatting and whitespace checks pass. Step 4a is complete; validation is next.

### Step 4b review

The user added try_new with the three agreed validation rules. The checks and
allowed zero-value cases are correct. Compilation currently fails because the
return type is spelled Return instead of Result. ConnectionWarmConfigError also
needs to be public so external CLI/server callers can use the public constructor.
The assistant updated the constructor documentation to describe its new validation
behavior; production logic remains user-written. Step 4b awaits these fixes and
focused validation tests.

Follow-up step 4b review: Result is spelled correctly and the error enum is now
public. The wire library compiles; formatting and whitespace checks pass. The
unused-field warning is expected until pool integration. Constructor logic is
approved; focused validation tests are the remaining part of step 4b.

Next user step: add ordinary synchronous tests in connection_warm.rs. Check zero
max_total and zero concurrency separately against their exact error variants;
check database and replication keys independently against the parameter error.
Exercise these rejections with per_database both zero and one to preserve the
rule that disabling warming does not bypass validation. Accept per_database zero,
Duration::ZERO, and an empty map with valid resource limits. Also accept a target
larger than max_total with an explicit user/application_name map and verify the
stored values remain unchanged. Nested unit tests can access the private fields;
do not expose setters or getters solely for these tests. No async runtime or
PostgreSQL is needed. Review before beginning CLI/server wiring.

Final step 4b verification: the user explicitly delegated these tests to the
assistant. Five synchronous tests were added, covering both reserved keys,
resource-limit errors with warming enabled and disabled, valid zero target/wait,
and preservation of supplied settings when the target exceeds global capacity.
All five tests pass, along with formatting and whitespace checks. The constructor
implementation is unchanged. Step 4b is complete; CLI/server wiring is next.

### Step 5: CLI and server configuration

All five planned CLI flags and server environment variables now map into
ConnectionWarmConfig::try_new. Startup parses the JSON profile into a
BTreeMap<String, String> and validates configuration before starting database
work. Defaults remain 0 spares, 32 global capacity, 4 concurrent attempts,
5000 ms initial wait, and an empty profile. The profile remains unresolved:
explicit users are preserved and a missing user is filled in by later pool work.

Tests cover defaults, all overrides (including milliseconds and explicit profile
values), zero target/wait, targets above the global cap, numeric parsing errors,
malformed or non-string JSON, reserved keys, and zero resource limits with
warming both enabled and disabled. ConnectionWarmConfig derives PartialEq/Eq
so entry-point tests can compare the complete validated value without exposing
its fields. Both apps use serde_json for profile parsing.

This step only wires configuration. The CLI validates it in ServeOptions::validate;
the server builds it before starting its engine. Neither entry point passes it
to listeners yet because the pool and lifecycle integration are later steps.
Help text and both READMEs document the settings; the READMEs explicitly state
that runtime warming is still pending. Step 6 is exact profile matching and
cold-path eligibility.

Verification: all 21 CLI/server binary unit tests pass with the pinned nightly
toolchain. Rustfmt and git diff --check pass. These tests require no PostgreSQL
instance or external Vitest run because this step only adds configuration parsing.

### Next user step: 6a — resolve the warm startup profile

Split step 6 into profile resolution and exact client matching. The user resumes
implementation; the step 5 delegation covered configuration wiring only.

In connection_warm.rs, add a crate-visible WarmStartupProfile type with a private
BTreeMap<String, String> of parameters and derive Clone, Debug, PartialEq, and Eq.
Add these methods:

```rust
impl ConnectionWarmConfig {
    pub(crate) fn resolve_profile(&self, default_user: &str) -> WarmStartupProfile;
}

impl WarmStartupProfile {
    pub(crate) fn parameters(&self) -> &BTreeMap<String, String>;
}
```

Resolve by cloning the validated configuration's startup_params and inserting
the configured PostgreSQL user only if the user key is absent. Preserve an
explicit user (including an empty value), every other key/value, and the original
configuration. Do not insert database: upstream startup receives the physical
database separately. This is a pure synchronous operation with no I/O and no
additional validation errors; configuration validation already rejects database
and replication. Resolution may run for a disabled configuration; the later
eligibility check handles the zero target.

Verify empty parameters resolve to just the default user; an explicit user wins;
application_name/options and explicit empty values survive unchanged; resolving
the same configuration with different default users does not mutate it. Stop
after this piece for review. Do not connect it to listeners or checkout yet.

Step 6b will match the complete forwarded client parameter map against this
resolved profile. Ignore only the routed database when comparing eligible normal
clients; replication remains rejected by connection handling and must never
qualify for a warm connection. Do not supply missing client parameters or compare
only a subset of keys. Missing, extra, or differing forwarded values mean cold
fallback; a zero warm count is ineligible regardless of profile equality. Keep
matching consistent with postgres_upstream::forwardable; actual checkout/cold
fallback integration remains step 13.

Step 6a review: the profile type, visibility, cloning, and borrowed accessor are
correct. try_insert preserves an existing user, including an empty value, but
its Result is currently ignored and triggers an unused_must_use warning. Prefer
entry("user".to_owned()).or_insert_with(|| default_user.to_owned()) to express
the fallback directly. Remove the unused futures::stream::BoxStream import.

The pinned-nightly focused suite compiles and all five existing configuration
tests pass; rustfmt and whitespace checks also pass. None of those tests calls
resolve_profile yet. Step 6a remains pending the resolution tests listed above
and the two warning cleanups. No production code was edited during this review.

Final step 6a review: the user replaced try_insert with entry/or_insert_with and
removed the unused import. At the user's request, the assistant added three
profile-resolution tests: an empty map becomes only the default user; explicit
users (including empty) and other values are preserved; repeated resolution with
different defaults leaves both the configuration and earlier profile unchanged.
All eight focused connection_warm tests pass. Rustfmt and whitespace checks pass.
The user's production logic is unchanged. Step 6a is complete; step 6b is next.

### Next user step: 6b — exact profile matching and eligibility

Add two pure, crate-visible methods in connection_warm.rs:

```rust
impl ConnectionWarmConfig {
    pub(crate) fn is_enabled(&self) -> bool;
}

impl WarmStartupProfile {
    pub(crate) fn matches(&self, client_params: &BTreeMap<String, String>) -> bool;
}
```

is_enabled returns whether per_database is greater than zero. matches returns
false whenever the client map contains replication, including an empty or false
value. Otherwise compare the entire forwarded client map with connection_params.
Make postgres_upstream::forwardable pub(crate) and reuse it so matching follows
the same filtering as send_startup. Do not change the filtering rules.

Only the routed database is ignored for normal clients. All forwarded keys and
values must match exactly: missing user, extra keys, omitted keys, different
case, or different values are mismatches. Do not fill in client defaults or
normalize options/encoding. The configured warm profile already has its default
user resolved. Input maps must remain unchanged.

The later checkout path will require both config.is_enabled() and
profile.matches(client_params). Disabled warming or mismatches use cold startup;
replication remains an error in the existing connection handler, not an accepted
cold connection. Database routing, control connections, and lease assignment
remain the handler's responsibility. Do not wire checkout into it until step 13.

Verify enabled/disabled counts, exact matches with differing routed database
names, changed/missing/extra forwarded parameters, missing versus empty user,
case-sensitive values, replication presence regardless of value, and unchanged
input. These are synchronous unit tests requiring no PostgreSQL. Stop for review
after these helpers and their tests; pool storage is step 7.

Step 6b tests added by the assistant as requested: seven tests cover positive
versus zero targets (independent of startup wait), exact matching with different
routed databases, missing/changed/extra forwarded parameters, case-sensitive
keys and values, missing versus empty user, replication presence regardless of
value, and preservation of both inputs. No production methods or stubs were
added. The focused test command currently fails with E0599 for the absent
is_enabled and matches methods, as expected for this tests-first handoff.
Formatting and whitespace checks pass. Step 6b awaits the user's implementation
and a passing test run.

Step 6b review: is_enabled and the replication guard are correct. The matcher
currently iterates only the configured profile, so it checks subset containment
instead of exact equality and accepts additional client parameters. A user-only
profile could incorrectly match a client with extra options/application_name.
The focused suite reports 14 passing tests and one failure:
extra_forwarded_parameters_do_not_match_even_when_empty. Make forwardable
pub(crate) and compare its complete output against connection_params after the
replication guard. Remove the unused BTreeSet import. Formatting and whitespace
checks pass. Step 6b remains pending this correction; production code was not
edited during review.

Final step 6b review: matches now compares the complete forwarded map after
rejecting replication. The forwarding helper is crate-visible and its filtering
rules are unchanged; the unused import is removed. All fifteen focused
connection_warm tests pass, along with formatting and whitespace checks.
Step 6b is complete. Runtime eligibility/checkout integration remains step 13;
the next implementation step is pool storage and single-use checkout (step 7).

### Next user step: 7a — pool state and construction

Step 7 will be split into state/construction, reservation/publication, and atomic
checkout. Start with types and an empty constructor in connection_warm.rs:

```rust
pub(crate) struct ConnectionWarmPool {
    config: ConnectionWarmConfig,
    profile: WarmStartupProfile,
    state: std::sync::Mutex<WarmPoolState>,
}

#[derive(Default)]
struct WarmPoolState {
    databases: std::collections::HashMap<DatabaseId, DatabaseWarmState>,
    capacity_used: usize,
}

struct DatabaseWarmState {
    database_name: String,
    idle: std::collections::VecDeque<UpstreamSession>,
    in_flight: usize,
}
```

Import the existing DatabaseId from
pgtest::worker_engine::database_jobs and UpstreamSession from
crate::postgres_upstream. DatabaseId is already public and derives Hash/Eq;
it does not derive Ord, so use HashMap rather than BTreeMap. IDs are unique
within an engine lifetime; one pool belongs to one engine. Do not invent a new
identity or key entries by lease ID/name. Keep the stream and startup response
bytes together inside UpstreamSession; do not clone sessions. Pool ownership will
be shared via Arc when integrated, not by cloning its state.

Add pub(crate) fn new(config: ConnectionWarmConfig, default_user: &str) -> Self.
Resolve the profile once, retain the validated config, and initialize an empty
mutex-protected state with zero capacity_used. Construction must not create
databases, open sockets, or spawn tasks, including when warming is enabled.
Test empty state, retained config, resolved default/explicit user, and successful
construction with disabled warming. Stop here for review.

Upcoming step 7 rules: reserve capacity under the state lock before opening a
connection. capacity_used counts idle sessions plus outstanding attempts globally;
each database's idle.len() + in_flight is bounded by its configured target.
Publishing a successful reserved attempt changes in-flight to idle without
increasing capacity_used. Failure/cancellation must release a reservation once,
including dropped futures; cancellation must not release capacity before the
corresponding connection attempt/socket is finished. Define reservation ownership
in the next substep before implementing these transitions.

Checkout will synchronously check enablement/profile, then remove one session for
the supplied DatabaseId and update accounting under the same mutex. Return the
owned session after releasing the lock; never return used sessions to storage.
Unknown IDs, empty queues, or mismatches return None without opening connections
or waiting for replenishment. Keep socket I/O, awaits, and disposal of drained
sockets outside lock scopes. Retirement/drain coordination and health monitoring
will extend this state in their later steps; this skeleton is not a complete pool.

Step 7a review: the state fields and constructor correctly retain configuration,
resolve the profile once, and initialize empty state without I/O. The mutex import
is currently tokio::sync::Mutex; change it to std::sync::Mutex as planned for
short synchronous state transitions and later reservation cleanup from Drop.
No lock will be held across an await. The existing fifteen focused tests pass,
and formatting/whitespace checks pass, but none constructs ConnectionWarmPool.
Step 7a awaits the mutex change and constructor tests for empty state, retained
configuration, default/explicit user resolution, and disabled warming. Production
code was not edited during this review.

Final step 7a verification: the user switched to std::sync::Mutex. The assistant
added three synchronous constructor tests as requested, covering default disabled
warming, enabled configurations with both zero and positive startup waits,
preservation of all settings, default-user insertion, explicit/empty users, and
empty database storage with zero capacity used. All eighteen focused tests pass;
formatting and whitespace checks pass. Constructor logic is unchanged. Step 7a
is complete. Unused DatabaseWarmState fields are expected until pool operations
are implemented.

### Next user step: 7b.1 — register a database for warming

Before reserving attempts, add this synchronous method on ConnectionWarmPool:

```rust
pub(crate) fn register_database(
    &self,
    database_id: DatabaseId,
    database_name: String,
) -> bool;
```

Return false immediately when warming is disabled. Otherwise lock state and use
the map entry API. For a vacant DatabaseId, insert DatabaseWarmState with the
supplied physical name, an empty idle queue, and zero in_flight, then return true.
For an occupied ID, return false without replacing the existing name, queue, or
counters, even if the new name differs. The return value means newly registered,
not connection readiness. Keep capacity_used unchanged: registering metadata does
not reserve a backend slot. Do not limit the number of registered databases by
max_total; that cap applies to connections and attempts, not database records.

Use a short std::sync::Mutex critical section, with no await, socket operations,
or spawned work. An explicit expect on mutex poisoning is acceptable for this
internal invariant; do not silently recover possibly inconsistent counters.
The caller will supply successful database-creation notifications in steps 8–9;
leave core and listeners unchanged for now. Database IDs, not names, distinguish
entries; IDs must not be reused within this pool's engine lifetime.

Tests should verify disabled registration is a no-op; enabled insertion stores
the exact ID/name with empty state; duplicate IDs preserve the original entry;
distinct IDs are independent (including equal names); and more databases than
max_total can be registered while capacity_used stays zero. Stop for review after
registration and its tests. Step 7b.2 will define owned reservations and their
exactly-once cleanup before adding publication.

Step 7b.1 tests added: five synchronous tests cover disabled registration, exact
identity/name storage with empty state, preservation of duplicate entries and
outstanding-attempt accounting, independent IDs including equal names, and
registration beyond the connection cap without consuming capacity. The last case
also verifies registration when connection capacity is already fully reserved.
Tests seed in_flight and capacity_used directly until the reservation API exists;
preservation of populated socket queues can be exercised with publication later.
Only tests were added to production source. The focused command currently fails
with E0599 because register_database is not yet implemented. Formatting and
whitespace checks pass. Step 7b.1 awaits implementation and a passing test run.

Final step 7b.1 review: register_database correctly rejects disabled warming,
inserts only vacant identities, preserves occupied entries, and leaves connection
capacity unchanged. The user chose to log and return false on mutex poisoning;
this is acceptable here because no poisoned state is accessed or recovered.
Future reservation cleanup/draining needs its own explicit poison policy.
All twenty-three focused tests pass; formatting and whitespace checks pass.
Step 7b.1 is complete. No production code was changed during review. Owned
capacity reservations and cleanup (7b.2) are next.

### Next user step: 7b.2 — owned reservations with automatic release

Add Arc to the std::sync imports and define a crate-visible, non-Clone
WarmReservation in connection_warm.rs:

```rust
#[must_use]
pub(crate) struct WarmReservation {
    pool: Arc<ConnectionWarmPool>,
    database_id: DatabaseId,
    database_name: String,
    active: bool,
}
```

Add these signatures:

```rust
impl ConnectionWarmPool {
    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        database_id: DatabaseId,
    ) -> Option<WarmReservation>;
}

impl WarmReservation {
    pub(crate) fn database_name(&self) -> &str;
}

impl Drop for WarmReservation {
    fn drop(&mut self);
}
```

Keep ConnectionWarmPool::new returning Self. Callers wrap it in Arc when they
need reservations; a reservation keeps the pool alive without borrowing it or
holding its mutex. It owns one outstanding capacity claim, not a socket.

try_reserve first rejects disabled warming. Under one short state lock, reject
unknown IDs, capacity_used >= config.max_total, or
entry.idle.len() + entry.in_flight >= usize::from(config.per_database). These
failures return None without changing state. On success, clone the registered
physical name, increment entry.in_flight and capacity_used once under that same
lock, and return an active reservation owning Arc::clone(self). Release the lock
before returning. Do not wait for a free slot, open a socket, or spawn work.
The concurrency setting limits actual connection attempts in step 11; it is not
an additional reservation cap in this step.

Drop of an active reservation locks state and decrements the matching database's
in_flight and global capacity_used once, without touching idle sessions or other
entries. Inactive reservations do nothing; publication will later deactivate a
reservation after transferring its accounting to an idle session. Do not expose
manual counter updates or derive Clone for reservations. A registered database
entry must remain present while reservations are outstanding; later retirement
will disable the entry and wait for attempts before removing it. Use invariant
checks for missing entries/underflow rather than saturating subtraction. Calculate
both decremented values before assigning them so an invariant failure cannot
leave a partially updated count.

Follow the current fail-closed mutex-poisoning policy: try_reserve logs and returns
None; Drop logs and returns without accessing poisoned state. Do not unwrap a
poisoned mutex in Drop (which could double-panic during unwinding) or recover it
with into_inner. A poisoned pool remains unusable for future registration and
reservation; ordinary cleanup guarantees assume unpoisoned state.

Test unknown/disabled reservations, increments and release on drop, per-database
and global limits independently, capacity reuse after drop, independence of
database counters, preservation of the registered name, and simultaneous callers
not exceeding capacity. Keep a winning reservation alive while checking a racing
caller so the test does not mistake valid reuse for over-allocation. A synchronous
scope-unwind test can verify automatic release. No PostgreSQL or sockets are
needed. When actual connection attempts are added, their socket/future must be
dropped before its reservation is released. Publication is a separate next piece;
stop after reservation creation, Drop, and tests for review.

Step 7b.2 tests added by the assistant: nine tests in
connection_warm::tests::reservations cover disabled/unknown IDs, registered-name
preservation, increments and release on drop, independent per-database/global
limits and slot reuse, pool ownership through Arc, release during scope unwinding,
inactive reservation drop, the agreed mutex-poisoning behavior, and concurrent
callers respecting either limit. Concurrent results remain owned until all
callers finish; no sleeps or sockets are used. Counter assertions also verify
the state mutex is not retained by reservations and other database records remain
unchanged. The inactive test simulates prior settlement until publication exists;
tests with populated idle queues will accompany publication.

No implementation or stubs were added. The focused test command fails with E0599
for the missing try_reserve method, as expected for this tests-first handoff.
Formatting and whitespace checks pass. Step 7b.2 awaits the user's implementation
and a successful test run.

Step 7b.2 review: code compiles, but all nine new reservation tests fail; the
previous twenty-three tests pass. try_reserve increments counters and then returns
None, leaking the claim because no WarmReservation owns it. Return an active
reservation with Arc::clone(self), the requested ID, and the registered physical
name. Rename the DatabaseId argument from database_name to database_id to avoid
confusing it with the cloned String. Capacity checks already bound increments,
so saturating_add is unnecessary.

Drop must guard !self.active rather than disabled configuration; the current code
would release an already-settled reservation again. Replace saturating_sub and
entry(...).and_modify with checked decrements and an explicit missing-entry
invariant. Compute both decremented values before assigning either, retaining the
same state lock. The poisoning branches and early capacity checks are appropriate.
Formatting and whitespace checks pass. No production code was edited during
review. Step 7b.2 remains pending these fixes and another focused test run.

Follow-up step 7b.2 review: try_reserve now returns an owned active reservation,
the ID argument is correctly named, and Drop checks active. All thirty-two
focused tests pass, as do formatting and whitespace checks. The remaining review
item is checked accounting: Drop still uses saturating_sub and silently skips a
missing database after changing global capacity. Replace this with explicit
missing-entry/underflow checks and calculate both new counts before either write.
The current tests exercise valid states, not these broken-invariant cases.
Saturating increments are also unnecessary after the capacity guards. Step 7b.2
remains pending this final invariant-handling correction; no production code was
edited during review.

At the user's request, the assistant added three Drop regression tests before
the accounting fix: missing database, zero global capacity, and zero per-database
in_flight. Each expects an invariant panic and verifies that neither counter nor
database membership is changed. Tests inspect poisoned state only after catching
the destructor panic; production code retains its fail-closed policy. The run
reports thirty-two passing tests and these three expected failures against the
current saturating implementation. Formatting and whitespace checks pass. The
production fix remains for the user.

Next step 7b.2 review: checked decrements and missing-entry validation are now
present. Thirty-three tests pass, but two regression tests still fail because
capacity_used is assigned before checking database presence and in_flight.
Move state_lock.capacity_used = capacity_used after entry.in_flight = in_flight,
so both validations finish before either assignment. The global-underflow
regression already passes. Formatting/whitespace checks pass; production code
was not edited during review. Step 7b.2 remains pending the assignment-order fix.

Final step 7b.2 review: Drop now checks both counts and database presence before
assigning either decremented value. All thirty-five focused tests pass, including
the three invariant regressions, concurrent capacity enforcement, unwinding, and
poison handling. Formatting and whitespace checks pass. Saturating increments
remain redundant but cannot saturate after the existing capacity guards in a
valid state, so they do not block completion. Step 7b.2 is complete; publishing
ready sessions from reservations (7b.3) is next. No production code was changed
during review.

### Next user step: 7b.3 — publish a ready session

Add this consuming method on WarmReservation:

```rust
pub(crate) fn publish(mut self, session: UpstreamSession) -> bool;
```

true means the session was moved into the reservation's database idle queue;
false means it was rejected and closed. Consuming self prevents publishing twice
through the same reservation. The input must be the successfully authenticated,
idle session opened for this reservation's physical database and pool profile.
The future warming task will establish it through postgres_upstream::connect;
publication does not redo authentication, parse the burst, or perform I/O.

If self.active is false, close the supplied session and return false without
changing pool state. Otherwise acquire the state lock. Validate that global
capacity_used is positive, that the database entry exists, and that its in_flight
can be decremented with checked_sub(1). Complete all invariant checks before
changing counters or moving the session. A missing entry is still an invariant
violation at this stage; future retirement will retain entries until attempts
have settled and add explicit rejection of retired entries.

Under the same lock, append the entire UpstreamSession to entry.idle, assign the
checked decremented in_flight, and set self.active = false. Leave capacity_used
unchanged: the connection already owns the slot reserved for its attempt. Do not
apply the reservation limit checks again; publication must succeed when the
global/per-database capacity is full of valid reservations. Return true after
releasing the lock. The consumed reservation's Drop then sees inactive and does
not release the idle connection's capacity.

Follow the existing mutex-poison policy: log and reject without accessing the
poisoned state. Dispose of a rejected session after releasing any guard, before
the consumed reservation's Drop runs. Do not drop self while holding its pool's
mutex, which could deadlock on re-entry. One useful implementation structure is
to hold the input in an Option<UpstreamSession>, perform the locked transition
inside a block and take the session only on success, then drop any remaining
session outside that block before returning. Invariant panics should occur with
the state guard held so it becomes poisoned; the guard is released during
unwinding before reservation Drop sees that poison. No lock recovery is added.

Tests should cover the 1-in-flight/0-idle to 0-in-flight/1-idle transition with
constant global capacity, exact preservation of the socket and startup bytes,
unrelated database state, multiple reservations published without changing total
usage, publication at full capacity, and subsequent reservations counting idle
sessions toward both limits. Inactive/poisoned rejection must close the supplied
socket and preserve state. Cover missing database and counter invariants without
partial mutation, following the existing Drop regression approach. Tests may use
local socket pairs and synthetic startup bytes; no PostgreSQL instance is needed.
Keep real connection attempts, retirement integration, and checkout for later
steps. Stop after publication and its focused tests for review.

At the user's request, the assistant added ten publication tests in
crates/pgtest-wire/src/connection_warm/publication_tests.rs. They cover ownership
of the socket and startup bytes, transfer from in-flight to idle at full capacity,
multiple reservations and queue order, idle sessions counting toward both limits,
duplicate registration preserving queued sessions, inactive/poisoned rejection
closing sockets, and three invariant failures without partial state changes.
Formatting and whitespace checks pass. The focused test build fails as expected
because WarmReservation::publish is not implemented; production implementation
remains for the user. Step 7b.3 remains pending implementation and passing tests.

Use the connection_warm:: filter to include both the existing tests and the new
publication test module:

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-wire --lib connection_warm::
```

First step 7b.3 implementation review: the focused suite compiles, with
thirty-nine tests passing and six failing. Formatting and whitespace checks pass.
Publication repeats the per-database reservation limit check, rejecting valid
reservations at their target, and returns false even after enqueueing a session.
Remove that limit check and return true after the successful transition. Add an
early inactive-reservation rejection before acquiring the lock; the current
inactive test passes only because its in_flight count is zero, while another
outstanding reservation could otherwise be consumed. Validate positive global
capacity, database presence, and checked in_flight decrement under the guard
before any mutation. Missing database and zero in_flight currently panic only
later in Drop; detect them directly during publication instead. Global capacity
must remain unchanged. Step 7b.3 remains pending these fixes and another focused
test run. No production code or tests were edited during review.

Follow-up step 7b.3 review: all forty-five focused tests pass, along with
formatting and whitespace checks. Inactive rejection, publication at capacity,
and the success return are fixed. One prior review item remains: the three
invariant failures still return false and rely on reservation Drop to panic.
That releases the publication guard before Drop reacquires it, leaving a window
for another thread to access invalid, not-yet-poisoned state. Assert positive
capacity and use expect for database presence and checked in_flight decrement
while the publication guard is held, before changing the queue or counters.
The current invariant tests observe the eventual panic and poison and therefore
do not distinguish publication validation from the later Drop panic. Step 7b.3
remains pending this correction. No production code or tests were changed.

Next step 7b.3 review: forty-five tests and formatting/whitespace checks still
pass. The latest change adds debug logs to the three invariant failure branches,
but each still returns false. Logging does not poison the mutex, so the gap
before Drop reacquires the lock remains. Replace these branches with the
previously specified assert/expect checks under the publication guard. Ordinary
inactive and already-poisoned rejection should continue to return false. Step
7b.3 remains pending; no production code or tests were changed during review.

Final step 7b.3 review: publication now asserts positive global capacity and
requires database presence and a checked in_flight decrement under the same
guard, before mutating state. Invariant panics poison the mutex before it is
released. Successful publication preserves global capacity and transfers the
entire session into the idle queue; inactive and poisoned rejection remain
unchanged. All forty-five focused tests pass, as do formatting and whitespace
checks. Steps 7b.3 and 7b are complete. Atomic single-use checkout (7c) is next.
No production code or tests were changed during review.

### Next user step: 7c — atomic single-use checkout

Purpose: hand one ready upstream session to a matching client, removing that
session from pool ownership and immediately freeing its capacity. This step
adds only the storage operation; the connection handler will call it in step 13.

Add this synchronous method to the ConnectionWarmPool impl in
crates/pgtest-wire/src/connection_warm.rs:

```rust
pub(crate) fn try_checkout(
    &self,
    database_id: DatabaseId,
    client_params: &BTreeMap<String, String>,
) -> Option<UpstreamSession>;
```

Some(session) transfers the entire socket and its startup response bytes to the
caller. None means there is no eligible spare; the future caller can use cold
startup. No connection attempt, query, await, or replenishment happens here.
The short synchronous mutex acquisition is allowed; never wait for an in-flight
warm attempt to finish. Do not change the existing session or stream types.

Implement the transition in this order:

1. Before locking, return None if config.is_enabled() is false or
   profile.matches(client_params) is false. Reuse the existing matcher: it
   requires the resolved user and every forwarded parameter to match, ignores
   the routed database parameter, and rejects any replication parameter. Do not
   fill in missing client parameters or mutate either map. Replication protocol
   errors remain the connection handler's responsibility.
2. Acquire state with the existing poison policy: log and return None on poison;
   never recover poisoned state in production.
3. Look up only database_id. Unknown ID or an empty idle queue returns None
   without changing anything. An in_flight count above zero does not make a
   session available. Never search other database entries, even if their names
   are equal.
4. For a nonempty queue, compute capacity_used.checked_sub(1).expect(...) while
   holding the guard and before popping the session. Zero capacity with an idle
   session is an invariant violation, not a normal miss: panic under the guard
   so the mutex is poisoned before another caller can access invalid state.
   Do not use saturating_sub or remove the session before this check.
5. Under that same guard, pop_front the session and assign the checked global
   count. The earlier nonempty check and held lock guarantee the pop succeeds;
   use expect to express that. Leave in_flight, database membership, names, and
   all other database entries unchanged. FIFO matches publication's push_back.
6. Return Some(session), releasing the guard as the method returns. The caller
   now owns the raw UpstreamSession independently of the pool. Its eventual
   drop closes the socket without changing pool counts or returning it to idle.

For borrowing, copy the old global capacity into a local before borrowing the
database entry mutably. Return on a missing/empty entry first, then compute its
checked decrement before pop_front. Once the entry's final use ends, assign the
new global capacity. Keep all these operations in one lock scope. There is no
reservation target/cap rejection on checkout; taking a spare frees a slot even
when the pool was full.

Example accounting: with one idle session and one in-flight attempt in this
database, checkout changes idle from 1 to 0 and global capacity from 2 to 1.
in_flight stays 1. A replacement can be reserved immediately while the client
still owns the checked-out socket. Closing that client socket later must not
decrement capacity a second time.

Ten tests are provided in
crates/pgtest-wire/src/connection_warm/checkout_tests.rs:

- Unknown, empty, and in-flight-only entries return an unchanged miss.
- Disabled checkout preserves even an artificially seeded spare.
- A matching checkout preserves startup bytes and socket identity, survives
  pool destruction, and leaves client parameters and the profile unchanged.
- Changed/missing/extra parameters and replication preserve the spare for a
  subsequent matching client.
- Explicit profile users, including an empty string, override the default user.
- FIFO checkout uses DatabaseId even when two entries share a database name.
- Checkout frees both limits immediately, preserves in-flight attempts, and
  never releases capacity again when the client-owned session is dropped.
- A poisoned mutex yields None without consuming or recovering state.
- Global underflow panics before removing the spare or changing any state.
- Eight concurrent callers receive two distinct spares exactly once, with
  correct final accounting.

The tests use local socket pairs and synthetic startup bytes, with shared
fixtures in connection_warm/test_support.rs; no PostgreSQL instance is needed.
They do not establish socket health: monitoring unused sessions is step 14.
Retirement eligibility, lifecycle hooks, replenishment, and handler integration
remain their later steps. Do not add any of them in this method yet.

Run the focused suite after adding the method:

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-wire --lib connection_warm::
```

The filter includes the existing forty-five tests and ten checkout tests. Stop
after this method and the focused checks for review; step 7c remains pending.

Step 7c tests added: formatting and whitespace checks pass. The focused test
build reports only missing try_checkout method errors, as expected before the
user's implementation; the new tests have not run yet. Existing publication
fixtures were moved into shared test support without changing their assertions.
No production method was implemented.

Step 7c implementation review: all fifty-five focused tests pass, including
concurrent checkout, exact matching, socket ownership, capacity accounting, and
underflow before queue mutation. Formatting and whitespace checks pass. The
implementation checks eligibility before locking, handles poison without
recovery, and performs removal and accounting under one guard. Steps 7c and 7
are complete. One nonblocking diagnostic typo remains: the checkout poison log
says "during publish"; change it to "during checkout". No production code or
tests were edited during review. Step 8 is next: the no-op core lifecycle
interface and DatabaseId in LeaseSession.

Follow-up: the checkout poison log now correctly says "during checkout".
Checkout behavior is unchanged, so the prior fifty-five passing tests remain
the latest behavioral verification; tests were not rerun for this log-only
correction. Whitespace checks pass. Step 7 has no outstanding review findings.

### Next user step: 8a — carry the physical database identity

Purpose: wire checkout needs the physical DatabaseId selected by the engine.
The client lease ID, lease generation, and database name are different values;
none should be used to reconstruct the physical ID. This step carries the
existing inventory identity through the successful attach path.

Make these changes in order:

1. In crates/pgtest-core/src/worker_engine/messages.rs, import DatabaseId and add
   database_id: DatabaseId to ConsumerReply::Attached. Keep the name, generation,
   and cancellation fields.
2. In WorkerEngine::reply_attached in worker_engine/core.rs, fill that field from
   entry.database.database_id. The common reply function handles both a new
   assignment and joining an existing lease, including queued attachments.
   Copy the existing ID; do not allocate one here or use entry.generation.
3. In worker_manager/worker_io.rs, import DatabaseId and add
   pub database_id: DatabaseId to LeaseSession. Add database_id as the first
   argument of its private new constructor and store it directly.
4. In ConsumerWorker::reply, bind database_id from ConsumerReply::Attached and
   pass *database_id as the first argument of LeaseSession::new. Preserve the
   existing failed-delivery handling, detach_on_drop flag, and cancellation
   token behavior. Drop must still send Detach using lease ID and generation,
   not the physical database ID.
5. Update existing Attached test literals with a DatabaseId. In reply_tests.rs,
   use an ID distinct from generation 42, for example DatabaseId(77). Existing
   exhaustive destructuring that does not inspect the new field can add `..`.
   Search all call sites rather than changing production semantics to satisfy
   a compile error:

   ```sh
   rg -n 'ConsumerReply::Attached|LeaseSession::new|LeaseSession\s*\{' crates apps
   ```

The existing fields remain available to callers, so wire connection handling
should not need behavioral changes. Stop after this propagation for review.
Do not add the lifecycle trait or wire checkout into connection handling yet;
those are separate steps. DatabaseId is unique within one engine, so later the
pool must remain scoped to that engine.

Run the existing fake-engine and reply regression tests; these do not require
PostgreSQL or Docker:

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-core --lib worker_engine::
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-core --lib worker_manager::reply_tests::
```

Per the user's revised testing preference, no new tests were added for step 8.
Once the connected lifecycle path is available, add integration coverage in the
existing manager/PostgreSQL harness: created database identity reaches attached
sessions, joining a lease preserves it, replacement after expiry gets a new ID,
and retirement disables that same pool entry before cleanup. Retain the existing
drop/failed-delivery regression coverage and use the external Vitest suite for
the full client workflow. This deferred coverage is not a completed validation
claim; step 8a and lifecycle integration remain pending.

Step 8a review: the engine copies entry.database.database_id into Attached,
ConsumerWorker forwards it into LeaseSession, and the session exposes it
publicly. Cancellation, failed-delivery rollback, and detach's lease/generation
identity remain unchanged. All forty-nine existing engine tests and both reply
tests pass, along with formatting and whitespace checks. Step 8a is complete.
New identity/lifecycle integration coverage remains deferred per the user's
preference; these existing tests do not directly assert the new session field.
No production code or tests were changed during review. Step 8b, the optional
core lifecycle interface and installation before initialization, is next.

### Next user step: 8b — install an optional lifecycle hook

Purpose: allow pgtest-wire to observe physical database availability and
retirement without introducing a dependency from core to wire. Install the
hook when constructing the engine, before try_init creates initial databases.
This step introduces the interface and passes the same hook through startup;
steps 9 and 10 will call it at the correct creation and retirement transitions.

1. Create crates/pgtest-core/src/worker_engine/lifecycle.rs and export it with
   pub mod lifecycle in worker_engine/mod.rs. Define this object-safe interface:

   ```rust
   pub trait DatabaseLifecycle: Send + Sync + 'static {
       fn database_ready(&self, database_id: DatabaseId, database_name: &str);
       fn database_retired(&self, database_id: DatabaseId);
   }

   #[derive(Default)]
   pub struct NoopDatabaseLifecycle;
   ```

   Import DatabaseId from database_jobs. Implement both methods for
   NoopDatabaseLifecycle with empty bodies. Document that database_ready will
   report a successfully accepted physical database, and database_retired will
   report logical retirement before cleanup is submitted. Methods are
   synchronous notifications: implementations must finish promptly, without
   socket I/O, DDL, or waiting for background work. Clone the borrowed name if
   retaining it after the callback. Background warming failures are handled
   by the implementation; they do not become engine startup/DDL errors.
   Drain coordination will be added separately in the later cleanup steps;
   database_retired is not an acknowledgement that all sockets have closed.

2. In worker_engine/core.rs, add a private
   lifecycle: Arc<dyn DatabaseLifecycle> field to WorkerEngine. Keep the current
   new constructor signature for existing callers. Add:

   ```rust
   pub fn new_with_lifecycle(
       pool_worker_config: WorkerEngineConfig,
       postgres_manager: Arc<Postgres>,
       engine_io: IO,
       inbox: Inbox,
       lifecycle: Arc<dyn DatabaseLifecycle>,
   ) -> Self;
   ```

   Move the existing constructor body into new_with_lifecycle and store the
   supplied Arc. Make new delegate to it with Arc::new(NoopDatabaseLifecycle).
   Keep initialization in one place. Retain the existing four generic type
   parameters; no additional lifecycle generic, global hook, or setter is
   needed. No callback is invoked during construction.

3. In worker_manager.rs, add a public startup method:

   ```rust
   pub async fn start_with_lifecycle(
       postgres_config: PostgresConfig,
       worker_engine_config: WorkerEngineConfig,
       lifecycle: Arc<dyn DatabaseLifecycle>,
   ) -> Result<Self, StartError>;
   ```

   Move start's current validation/preparation/startup body into this method.
   Preserve validation of max_lease_records before preparing PostgreSQL.
   Pass the supplied Arc to startup::start_workers. Keep start's original
   signature and make it delegate with Arc::new(NoopDatabaseLifecycle). Existing
   CLI, server, and test callers continue using start. The later wire setup
   will opt in by passing its pool as the hook.

4. In worker_manager/startup.rs, add lifecycle: Arc<dyn DatabaseLifecycle> as
   the final parameter of the existing private start_workers function. Use
   WorkerEngineType::new_with_lifecycle with this Arc instead of new. The hook
   must already be stored when Box::pin(worker_engine.try_init()).await runs.
   Preserve that Box::pin, existing error propagation, task tracking,
   cancellation, and the order of spawning workers. No extra Arc field is
   needed in WorkerEngineManager: the engine owns it for its lifetime.

5. Update the existing test that calls startup::start_workers directly to pass
   Arc::new(NoopDatabaseLifecycle). All ordinary WorkerEngine::new and
   WorkerEngineManager::start callers should still compile unchanged. Find the
   internal startup call sites with:

   ```sh
   rg -n 'start_workers\(' crates/pgtest-core
   ```

The resulting startup path is start_with_lifecycle -> start_workers ->
new_with_lifecycle -> try_init. The original start and new entry points supply
the no-op implementation. Preserve the caller's supplied hook throughout this
path; do not replace it with a no-op in an intermediate function.

Stop after the hook is stored before initialization. Do not yet call the hook,
implement it for ConnectionWarmPool, or change database creation/retirement.
The unused lifecycle field is expected temporarily. New integration tests remain
deferred until notifications and their consumers can be exercised together.

Verification uses the existing engine/reply tests from step 8a, plus the startup
stack-size and validation regressions. These exact tests require no PostgreSQL
or Docker:

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-core --lib worker_manager::worker_engine_manager_test::startup_future_fits_stack_budget -- --exact
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-core --lib worker_manager::cleanup_tests::zero_lease_record_limit_is_rejected_before_connecting -- --exact
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-core --lib worker_manager::cleanup_tests::startup_preserves_postgres_configuration_errors -- --exact
```

This guide adds no implementation or tests. Step 8b remains pending the user's
changes and review; notification ordering and hook integration are not yet
validated by the existing tests.

Step 8b review: the injected hook is stored in the engine before try_init, and
the trait is public, object-safe, Send + Sync + 'static with a no-op
implementation. All fifty-four existing checks pass (forty-nine engine, two
reply, startup stack size, and two startup-validation regressions). Formatting
and whitespace checks pass. These tests still exercise the original startup
entry point; injected-hook integration remains deferred.

Before completing 8b, consolidate the duplicated implementations: WorkerEngine::new
should delegate to new_with_lifecycle with a no-op; WorkerEngineManager::start
should delegate to start_with_lifecycle with a no-op; and start_workers should
delegate to start_workers_with_lifecycle with a no-op if keeping both private
functions. Keeping that private compatibility wrapper is fine and avoids
changing its existing test caller. Keep each full construction/startup body only
in the corresponding with_lifecycle function. This lets the existing regression
tests exercise the same setup path that injected hooks will use and prevents
the two paths from diverging as lifecycle integration grows.

Remove the unused lifecycle::self import, use the NoopDatabaseLifecycle import
in the delegating wrapper, and prefix unused no-op implementation parameters
with underscores. The unread engine lifecycle field remains expected until
step 9. Add the callback contract as trait documentation: notifications must
finish promptly without I/O/waits; retirement precedes cleanup submission and
does not mean draining is complete. No production code or tests were changed
during review. Step 8b remains pending consolidation.

Follow-up 8b review: the build now fails with seven compile errors. The engine's
five-argument constructor was renamed to new, but its four-argument callers
remain and startup still calls new_with_lifecycle. Rename the full constructor
back to new_with_lifecycle and restore new as a four-argument delegating wrapper.
Delegation keeps both entry points while sharing one full implementation.

The public start_with_lifecycle was also removed; restore it so callers can
supply a real hook. Move validation/preparation into it and make the existing
two-argument start delegate with the no-op. The private three-argument
start_workers is fine: keep it and update its direct startup-failure test call
to pass Arc::new(NoopDatabaseLifecycle). Its engine construction should call
new_with_lifecycle with the supplied hook. Formatting and whitespace checks
pass, but no tests ran because compilation failed. No production code or tests
were edited during review; step 8b remains pending.

Next 8b review: compilation is fixed and all fifty-four existing checks pass;
formatting and whitespace checks pass too. The user retained a single
five-argument WorkerEngine::new and updated its internal callers. That structure
is acceptable: it retains one constructor implementation and installs the hook
before initialization. A separate internal new_with_lifecycle wrapper is no
longer required for this chosen structure.

The remaining functional gap is the public startup API. WorkerEngineManager::start
still always creates NoopDatabaseLifecycle, and start_with_lifecycle is absent,
so wire cannot supply its real pool. Add public start_with_lifecycle with the
two existing configuration arguments plus Arc<dyn DatabaseLifecycle>. Move
start's current body into it and pass the supplied hook to start_workers. Make
the existing start delegate with the no-op. Keep the current internal new and
start_workers signatures unchanged. The no-op parameter/import warnings and
trait contract documentation remain small cleanups; the unread engine field
is expected until step 9. No production code or tests were changed during
review. Step 8b remains pending public hook injection.

Final 8b review: public start_with_lifecycle now owns validation and startup,
passes the supplied Arc through start_workers into WorkerEngine::new, and
installs it before try_init. The original start delegates with the no-op hook.
There is one full startup path and one internal engine constructor. All
fifty-four existing checks pass, including startup stack size and validation,
as do formatting and whitespace checks. Steps 8b and 8 are complete.

The unused lifecycle::self import and no-op method arguments remain nonblocking
compiler warnings; the unread engine lifecycle field is expected until step 9.
The callback contract is documented in this guide but still needs rustdoc on
the public trait. No production code or tests were changed during review.
Creation/retirement callbacks and their integration coverage remain future work;
the next step is 9, notifying successful initial and runtime database creation.

### Next user step: 9 — notify accepted database creation

Purpose: announce each newly available physical database to the installed
lifecycle hook, including unused initial databases and runtime growth. Change
only the two engine creation-completion paths in worker_engine/core.rs.
DatabaseInventory::complete_creation remains the authority on whether a
completion is accepted; keep its API and identity bookkeeping unchanged.

Use this ordering for each creation result:

1. Before moving result into complete_creation, retain its successful name:

   ```rust
   let database_name = result.as_ref().ok().cloned();
   ```

   This clones only the Arc-backed ReadString, not a database or connection.
   Saving a successful name does not itself authorize notification: a duplicate
   or unknown completion may contain Ok(name) but still be rejected by inventory.
2. Call inventory.complete_creation(database_id, result) exactly once.
3. Only for an accepted Ok(()) outcome, retrieve the saved name with
   expect("accepted creation must have a database name") and call:

   ```rust
   self.lifecycle.database_ready(database_id, database_name.as_ref());
   ```

   Here database_name refers to the unwrapped ReadString. Pass the physical
   DatabaseId from the creation reservation/message and the exact created name.
   Do not use a lease ID, generation, or the template name.

In try_init:

- Save the name after create_database().await and before complete_creation.
- Keep the existing expect for a missing initial reservation.
- Handle the accepted result with a match: Ok(()) notifies; Err(error) preserves
  the existing error log and returns Err(error).
- Notify once inside the loop for each successful database, before continuing
  to the next creation and before try_init returns. Notify even when no lease
  is waiting. If a later creation fails, do not synthesize notifications for it
  or replay earlier notifications; leave existing startup-error behavior intact.

In handle_database_worker_message's CreationFinished arm:

- Save the successful name before moving result into complete_creation.
- Preserve the existing None branch: warn and return for an unknown or repeated
  completion. Neither successful nor failed ignored messages notify the hook.
- Match the accepted result. Ok(()) notifies; Err(error) retains the existing
  template_create_failures increment and error log without notifying.
- Keep dispatch_waiters() and grow() after this match. They must still run after
  accepted failures as they do today. Successful notification must occur before
  dispatch_waiters can hand that database to a waiting lease.

The outcome rules are:

| Completion | Notify | Existing follow-up |
| --- | --- | --- |
| Initial accepted success | Once | Continue initialization |
| Initial accepted failure | Never | Return startup error |
| Runtime accepted success | Once, before dispatch | Dispatch waiters, then grow |
| Runtime accepted failure | Never | Count/log failure, dispatch waiters, then grow |
| Runtime duplicate or unknown | Never | Warn and return |

Notifications belong at accepted creation, not in the DDL worker, inventory
type, attach/join, or return_ready. Reusing an unclaimed ready database does not
create another physical database and must not announce it again. Do not scan
the ready queue to recover the name or call complete_creation twice.

These synchronous notifications do not wait for warm sockets and do not change
DDL/startup error types. The no-op implementation preserves current runtime
behavior. Retirement callbacks, the pool's lifecycle implementation, and actual
warming remain later steps. Stop after these two notification sites for review.

Run the existing engine regressions (including failed, duplicate, unknown, and
queued creation cases):

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-core --lib worker_engine::
```

Those tests protect existing behavior but do not prove notification delivery
while using the no-op hook. Integration coverage remains planned: inject a
recording hook through the real manager startup path, verify initial events
exist when startup returns, then verify runtime creation is announced with the
same ID/name before its waiting attachment completes. Include failure and
duplicate handling through the existing deferred-worker harness as the structure
becomes ready. No new tests or production changes were added with this guide;
step 9 remains pending implementation and review.

Step 9 review: both creation paths notify only after inventory accepts a
successful result. Initial failures still return their error. Runtime failures
still count/log and continue to dispatch/grow, while ignored completions return
without notification. Runtime success notifies before dispatch_waiters. All
forty-nine existing engine tests pass, along with formatting and whitespace
checks. Step 9 is complete by code review and existing behavior regressions;
notification delivery integration coverage remains deferred as agreed.

Nonblocking cleanup: replace the temporary name_to_be_revemove_this_duplication
binding with a shadowed database_name, and prefer
database_name.expect("accepted creation must have a database name") in both
success paths for a useful invariant message. The current unwrap calls are
guarded by accepted success, so this does not change behavior. No production
code or tests were edited during review. Step 10 is next: notify retirement
before submitting cleanup.

Follow-up: runtime creation now shadows database_name and uses the explanatory
expect. Initial creation in try_init still has the temporary binding and unwrap;
apply the same cleanup there, including its log and callback references.
Formatting and whitespace checks pass. No behavioral tests were rerun for this
cleanup; the prior forty-nine passing engine tests remain the latest run.
Step 9 remains complete, with this nonblocking naming cleanup outstanding.

Final cleanup review: both creation paths now shadow database_name and use the
explanatory expect. Initial logging and notification reference that binding.
Formatting and whitespace checks pass; behavior is unchanged, so tests were
not rerun. Step 9 has no outstanding review findings. The latest behavioral
verification remains forty-nine passing engine tests, with notification
integration coverage still deferred as agreed.

### Next user step: 10 — notify retirement before cleanup

In `crates/pgtest-core/src/worker_engine/core.rs`, update `retire_lease` with
one callback after inventory records retirement and before cleanup is submitted:

```rust
let request = self.inventory.retire(entry.database);
let database_id = request.database_id;
self.lifecycle.database_retired(database_id);

if let Err(error) = self.engine_io.request_cleanup(request) {
    tracing::error!(?database_id, %lease, %error, "unable to enqueue cleanup; retaining retirement record")
}
```

Keep the existing lease removal guard, cancellation, and final `self.grow()`.
The order is: remove the assigned lease, cancel its token, mark the physical
database retiring, notify its ID, then submit cleanup.

Notify even if cleanup submission subsequently fails: the database is already
retired and its retirement record remains. Do not move the callback into the
successful cleanup branch or repeat it on cleanup completion. The existing
removal guard prevents a second notification for a lease with no assigned entry;
the expiry handler already checks generation before calling this method.

Do not notify on ordinary disconnect or when restoring an unclaimed database
to ready inventory. Those paths do not retire the physical database.

This synchronous callback must stay fast. It announces retirement; it does not
prove warm connections have drained. Wiring pool retirement and coordinating
drain before physical deletion belong to the later lifecycle integration,
including step 15. No new arguments or async hook are needed here.

After implementation, run the existing engine regressions:

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-core --lib worker_engine::
```

These tests exercise existing retirement behavior; their no-op hook does not
assert callback delivery or ordering. Keep that integration coverage deferred
as agreed, until the pool lifecycle is connected. Stop here for review before
step 11. This guide adds no production implementation or tests; step 10 remains
pending implementation and review.

Step 10 review: `retire_lease` now notifies the lifecycle hook with the physical
database ID after inventory records retirement and before submitting cleanup.
The removal guard and lease cancellation remain intact; cleanup submission
failure retains both the retirement record and the already-issued notification.
No review findings. All forty-nine existing engine tests pass, along with
formatting and whitespace checks. These tests use the no-op hook, so callback
delivery/order integration coverage remains deferred as agreed. Step 10 is
complete; step 11 will establish warm sessions with bounded concurrency,
timeouts, and cancellation. No production code or tests were edited in review.

### Next user step: 11a — one bounded warm connection attempt

Start with the network operation alone. `postgres_upstream::connect` already
returns an `UpstreamSession` only after authentication and a valid, idle
ReadyForQuery. Reuse it so the socket and its startup response bytes stay
together and TCP/Unix endpoint handling stays consistent.

Create `crates/pgtest-wire/src/connection_warm/attempt.rs` and declare
`mod attempt;` in `connection_warm.rs`. Add these types and function there:

```rust
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::WarmStartupProfile;
use crate::postgres_upstream::{self, UpstreamError, UpstreamSession};

const WARM_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub(super) enum WarmAttemptError {
    #[error("warm connection attempt cancelled")]
    Cancelled,
    #[error("warm connection attempt timed out")]
    TimedOut,
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
}

pub(super) async fn connect_warm_session(
    database_name: &str,
    profile: &WarmStartupProfile,
    upstream_host: &str,
    upstream_port: u16,
    cancellation: &CancellationToken,
) -> Result<UpstreamSession, WarmAttemptError> {
    // Implement the behavior below.
}
```

Implementation order:

1. Use `tokio::select!` with `biased;` and cancellation as the first branch.
   `cancellation.cancelled()` returns `Err(WarmAttemptError::Cancelled)`.
   This also prevents polling the connection attempt when the token is already
   cancelled.
2. The other branch awaits `tokio::time::timeout(WARM_ATTEMPT_TIMEOUT, ...)`
   around the entire `postgres_upstream::connect(database_name,
   profile.parameters(), upstream_host, upstream_port)` call. The deadline
   covers socket connection, authentication, and the wait for ReadyForQuery.
   Do not reset it between stages or use `config.startup_wait`: that setting
   controls the separate initial pool warm-up budget.
3. Match the nested timeout result: `Err(_)` becomes `TimedOut`,
   `Ok(Err(error))` becomes `Upstream(error)`, and `Ok(Ok(session))` returns
   the session. Preserve the existing upstream error as the source.

Keep the connection future directly inside the timeout/select. Do not spawn
it: cancellation or timeout must drop the future and its partially established
socket. Return the complete session without consuming, replacing, or replaying
its startup bytes. This helper performs one attempt, with no retry, publication,
lease attachment, or background task creation.

The caller will supply the registered physical database name and resolved pool
profile. The token will belong to the database's warm work, linked to pool
shutdown, rather than an individual client connection. Wiring that ownership
comes later. A cancellation check in select is not an atomic retirement guard:
the pool must still reject publication/checkout for retired databases under its
state lock when lifecycle integration is added.

Step 11b will add a semaphore shared by the pool, acquire its permit before
starting this helper, and hold an owned reservation through publication or
failure. Waiting for a permit must also be cancellable. The five-second network
deadline starts after permit acquisition. Reservation capacity and semaphore
permits have different purposes: capacity counts idle plus reserved sessions;
the semaphore limits simultaneous connection attempts. Scheduling/backoff is
step 12. Keep the helper disconnected from listeners and lifecycle callbacks
until the required lifecycle guards are ready.

Verification after implementing 11a:

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo check --locked --offline -p pgtest-wire
rustup run nightly-2026-09-10 rustfmt --check --edition 2024 --config skip_children=true crates/pgtest-wire/src/connection_warm.rs crates/pgtest-wire/src/connection_warm/attempt.rs
git diff --check
```

As agreed, defer new tests until they can exercise the connected attempt path.
That coverage must verify successful startup, upstream failure, timeout during
startup, cancellation while queued and connecting, and permit/reservation
recovery. Existing tests alone will not establish those new guarantees.
Stop after 11a for review. This guide adds no implementation or tests; both
substeps remain pending.

Step 11a review: the attempt correctly prioritizes cancellation, wraps the whole
upstream connection in one five-second timeout, preserves upstream errors, and
returns the intact session. Cargo check, formatting, and whitespace checks pass.
One integration prerequisite remains: change the private
`async fn connect_warm_ssesion` to `pub(super) async fn connect_warm_session`
so the parent pool module can call it in 11b. Also correct the constant's typo
from `WARMT_ATTEMPT_TIMEOUT` to `WARM_ATTEMPT_TIMEOUT` at its declaration and use.
The naming correction is nonbehavioral. No production files or tests were
edited during review; new attempt integration coverage remains deferred as
agreed. Resolve the helper visibility before advancing to 11b.

Step 11a follow-up: the helper is now `pub(super) connect_warm_session` and
both timeout constant references use `WARM_ATTEMPT_TIMEOUT`. No review findings
remain. Formatting and whitespace checks pass. These visibility/naming changes
do not alter behavior; the prior successful cargo check remains the latest
compilation check, and no behavioral tests were rerun. Step 11a is complete,
with new integration coverage still deferred as agreed. Step 11b is next.

### Completed step: 11b — establish and publish reserved sessions

`ConnectionWarmPool` now owns one semaphore shared across all its database
reservations. The effective permit count is the minimum of configured
concurrency, global capacity, and Tokio's semaphore limit. This preserves both
configured upper bounds without allowing unusually large valid settings to
panic in semaphore construction. Construction starts no background work.

`WarmReservation::establish(self, host, port, cancellation)` owns the reservation
through the entire operation. It waits cancellably for a permit, calls the 11a
helper with the registered physical name and resolved profile, checks for
observed cancellation, and publishes the complete session. The permit remains
held through publication. The five-second deadline begins after acquisition;
the initial `startup_wait` budget is independent.

The result is `Ok(true)` for publication, `Ok(false)` if publication rejects the
session, or a typed cancellation, timeout, upstream, or closed-semaphore error.
The existing reservation Drop releases capacity on failure or future abandonment;
permit and socket ownership provide the corresponding cleanup. Pool mutexes
are not held across asynchronous waits. There is no detached network task or
inline retry.

Six new local TCP scenarios cover the connected reservation/startup/publication
path: successful startup and socket handoff; a shared concurrency limit across
databases, queued/running cancellation and subsequent reuse; one timeout across
startup stages; upstream rejection; dropped queued/running futures; and
cancellation before opening a socket. Socket closure accepts EOF or TCP reset,
since dropping an unfinished connection with unread bytes can produce either.
These use a scripted wire peer, not a real PostgreSQL server or a mock lifecycle
hook. Full lifecycle/PostgreSQL/Vitest integration remains pending.

Verification: all sixty-one `connection_warm::` tests pass, including the six
new scenarios. Rust formatting and whitespace checks pass. Local TCP binding
was blocked by the sandbox, so socket verification ran with approved execution
outside it.

Step 11 is complete. Step 12 adds bounded fair scheduling and failure backoff.
Runtime integration must also provide database/pool cancellation ownership and
atomic retirement eligibility checks before enabling these attempts. The final
cancellation check in `establish` does not by itself serialize publication with
retirement. Draining before physical deletion and shutdown remain later steps.

### Next user step: 12a — fair selection of replenishment work

Step 12 turns one-shot attempts into background replenishment. Implement it in
four reviewable parts. Start with 12a below; it changes reservation selection
without opening connections or starting a scheduler.

In `crates/pgtest-wire/src/connection_warm.rs`, add a registration-order queue
to `WarmPoolState`:

```rust
schedule_order: VecDeque<DatabaseId>,
```

`Default` can still be derived. In `register_database`, append the ID only when
the vacant entry is inserted successfully. Duplicate registration, disabled
warming, and lock failure must leave the queue unchanged. Keep one queue entry
per registered database; checkout and completion must not append duplicates.

Extract the reservation transition currently inside `try_reserve` into a private
helper with this shape:

```rust
fn reserve_locked(
    self: &Arc<Self>,
    state: &mut WarmPoolState,
    database_id: DatabaseId,
) -> Option<WarmReservation>
```

The helper must use the supplied state and never lock `self.state` itself. Keep
the existing capacity limits, database lookup, in-flight increment, capacity
increment, and owned reservation construction. Keep the disabled guard in both
public reservation entry points. `try_reserve` still takes the mutex once and
delegates to this helper, preserving existing callers and tests.

Add the fair entry point:

```rust
pub(crate) fn reserve_next(self: &Arc<Self>) -> Option<WarmReservation>
```

Its algorithm is:

1. Return `None` if warming is disabled or the state lock is poisoned.
2. Take the state lock once. If global capacity is full, return `None` without
   rotating the queue.
3. Capture the queue's current length as the maximum number of candidates to
   inspect during this call.
4. Pop the front ID. If its database no longer exists, discard the stale queue
   entry and continue. Otherwise push it to the back before trying to reserve.
5. Call `reserve_locked` for that ID. Return the first successful reservation;
   otherwise continue scanning, at most the captured number of candidates.
6. Return `None` if that pass finds no capacity to fill.

Selection and reservation must occur under the same lock. Do not call the public
`try_reserve` while holding the mutex: it would try to lock it again. Do not
hold a reservation and then drop it while the mutex is held, because its Drop
also locks the pool. Returning a successful reservation transfers it out safely.

For registration order A, B, C with target two and enough global capacity,
successive calls should reserve A, B, C, A, B, C. If A already meets its target,
skip it and serve B/C. A full pass with no eligible work must terminate. The
cursor survives across calls so a newly freed slot does not always favor the
same first database. This is fairness when allocating available capacity; it
does not evict already-warm sessions to redistribute a full pool.

If any test constructs `WarmPoolState` directly, initialize its new field.
Preserve all existing reservation/publication/checkout behavior. There are no
retry fields, timer tasks, lifecycle callbacks, or network calls in 12a.

Verification after implementing 12a:

```sh
RUSTC=/Users/said/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rustup run nightly-2026-09-10 cargo test --locked --offline -p pgtest-wire --lib connection_warm::
rustup run nightly-2026-09-10 rustfmt --check --edition 2024 --config skip_children=true crates/pgtest-wire/src/connection_warm.rs
git diff --check
```

The existing sixty-one tests protect current behavior but do not establish
round-robin fairness. Following the integration preference, cover that behavior
through the connected scheduler once 12d is ready. Stop after 12a for review.

### Remaining step 12 sequence

**12b — retry state (implemented).** Each database stores
`retry_at: Option<tokio::time::Instant>` and a `tokio-retry2`
`ExponentialFactorBackoff` iterator, initially with no deadline and a one-second
first delay. Shared reservation eligibility skips a database before its retry
deadline in both direct and round-robin reservation. An upstream error or
timeout records the next deadline before its reservation releases capacity.
The iterator supplies delays of 1, 2, 4, 8, 16, 30, 30 seconds. Successful
publication clears the deadline and resets the iterator under the state lock.
Already-running attempts can still finish; their outcomes update retry state
in the order they acquire that lock. The future scheduler must not apply the
same outcome a second time. Cancellation and rejected publication are not
upstream failures. A closed semaphore is a scheduler stop condition, not a
reason to retry repeatedly. Sleeping must hold neither reservation nor permit.

**12c — lifecycle eligibility and wakeups (implemented).** Add a pool cancellation token,
per-database child tokens, and an explicit retiring state. Retirement must mark
the database ineligible under the same mutex used by reservation, checkout,
and publication. Retain its inventory entry until all reservations settle;
the current reservation Drop requires it to exist. Drain idle sessions from
state with correct capacity accounting and dispose of them outside the lock.
Reject late publication without pushing a session, while releasing its reserved
capacity exactly once. Capture the database token with scheduled work so
retirement cancels waiting and active attempts. Add one `Notify` for the single
scheduler; registration, checkout, released capacity, and retirement wake it
to recheck state. Signal after releasing the state mutex. Wakeups request a
state recheck, rather than enqueueing one job per event. Logical retirement and
this notification mechanism do not replace the step 15 drain-before-drop barrier.

**12d — one bounded coordinator (implemented).** Own an endpoint, the shared pool, and a
`FuturesUnordered` of attempts in one scheduler future. Reserve work using
`reserve_next` only while the active-future count is below the effective attempt
limit. Each future owns its reservation and database token, calls `establish`,
and returns its database ID with the result. Keep the step 11 semaphore as the
shared connection limit. This bounds queued futures as well as sockets; do not
spawn one task per registered database or retry.

After processing completions and filling available slots, wait for completion,
notification, the earliest useful future retry deadline, or pool cancellation.
Do not busy-loop when all databases are full, cooling down, or retired. A retry
timer is useful only when global capacity and an attempt slot can be used;
ignore already-expired deadlines for entries blocked by other limits. With no
useful deadline, wait for a state change. On stop, drop/drain owned attempt
futures so their reservations, sockets, and permits settle. Start no scheduler
when warming is disabled; construction itself must remain free of spawned work.
Lifecycle/listener installation and full shutdown ownership remain separate
integration work. Do not enable runtime warming before retirement and physical
deletion coordination are connected.

Connected scheduler coverage should verify fair ordering and skipping,
replenishment after checkout, bounded work under many databases, retry delays
and success reset, progress for healthy databases while another backs off,
cancellation during retry/warm-up, and rejection of late retirement results.
Use controllable socket peers for timing/failure paths and the existing
PostgreSQL/Vitest harness for full lifecycle behavior. This update contains
instructions only; no step 12 implementation or tests have been added.

Step 12a completion: successful registration now appends its physical ID once.
`reserve_next` scans at most one queue rotation under the state mutex, discards
stale IDs, skips databases whose targets are covered, and uses the shared
`reserve_locked` transition. Global capacity exhaustion leaves the cursor
unchanged. Existing direct reservations retain their behavior. No network work,
retry policy, or scheduler was added.

All sixty-one existing warm-pool tests pass, along with formatting and whitespace
checks. Socket tests ran outside the sandbox with approval. These tests protect
existing behavior; round-robin selection itself was reviewed, and connected
fairness coverage remains deferred to 12d. Step 12a is complete. Step 12b is next:
per-database retry deadlines and capped exponential backoff.

### Completed step: 12b — tokio-retry2 backoff

Used the existing wire dependency on `tokio-retry2` 0.9.1. Its
[`ExponentialFactorBackoff`](https://docs.rs/tokio-retry2/0.9.1/tokio_retry2/strategy/struct.ExponentialFactorBackoff.html)
is configured with `from_millis(1_000, 2.0).max_delay(Duration::from_secs(30))`.
There is no jitter, preserving the agreed delay sequence, and no `Retry` task:
this step stores deadlines rather than sleeping with reserved resources.

Both reservation paths consult the same deadline check. Actual upstream errors
and timeouts advance the database's iterator before failure releases its slot.
Successful publication resets retry state atomically with making the session
available. Cancellation, dropped futures, closed-semaphore errors, and rejected
publication do not change backoff. Duplicate registration preserves retry state.
Other databases remain eligible during one database's cooldown.

This places outcome accounting in `establish`/`publish`, refining the earlier
plan to have the scheduler account for results: it closes the window between
capacity release and recording cooldown, and does not require scheduler code.
Future scheduler work must use the stored deadline and avoid counting results
again. Attempts already reserved before another failure are allowed to finish.

Added a socket-path regression that performs repeated rejected PostgreSQL
startups and verifies the complete delay sequence/cap, exact deadline
eligibility in direct and round-robin selection, progress for another database,
cancellation preserving an uncapped sequence, duplicate registration, and
success resetting the next failure to one second. Existing timeout and
cancellation scenarios now also check their effect on retry state.

All sixty-two warm-pool tests pass. The final strengthened cancellation scenario
also passes in a focused rerun. Formatting and whitespace checks pass. Only
12b was implemented; 12c lifecycle/wakeup work and the 12d scheduler remain
pending. Full PostgreSQL/Vitest lifecycle integration remains deferred.

### Completed step: 12c — retirement eligibility and wakeups

The pool owns a cancellation token and one `Notify`. Every registered database
gets a child cancellation token and an explicit `retiring` flag. Reservations
capture that database token when capacity is reserved. `establish` now observes
database/pool cancellation both while waiting for the semaphore and during
network startup; its existing caller token remains an additional cancellation
source and is never cancelled by the pool.

`retire_database(id)` atomically marks the physical database retiring, clears
its retry deadline, removes it from scheduling order, and extracts its idle
sessions while subtracting only their occupied capacity. After releasing the
mutex, it cancels the database token, closes the extracted sockets, and signals
the state change. Unknown or already-retired IDs are no-ops. Retired entries
remain present, including after their reservations settle, so duplicate
registration cannot reactivate that identity; removal will be coordinated with
the later cleanup/drain barrier.

Reservation, checkout, publication, and retry accounting now reject retired
databases under the same state mutex. A late publication closes its session
and lets reservation Drop release the remaining slot exactly once, outside the
original lock scope. Previously checked-out sessions stay with their clients;
existing core lease cancellation remains responsible for them. Pool cancellation
also prevents new admission and reaches the child attempt tokens, but complete
idle-pool shutdown/drain ownership remains step 16.

Successful registration, checkout, publication, recorded failure, reservation
release, and retirement call `notify_one` after unlocking state. Notifications
coalesce into a state recheck for the future single scheduler rather than
allocating a job per event. No-op registration/retirement does not signal.

Four new socket scenarios verify retirement of queued/running attempts without
cancelling another database, parent cancellation propagation, idle socket
closure and late-publication rejection while handed-off traffic still works,
wakeups/capacity recovery/idempotence, and retirement during retry backoff.
The existing virtual-time timeout test now allows a ten-millisecond timer
scheduling margin rather than assuming ten yields finish the task; it still
rejects a seconds-long timeout restart after authentication.

All sixty-six warm-pool tests pass, with local TCP access approved outside the
sandbox. Formatting and whitespace checks pass. Step 12c is complete. No
scheduler, core lifecycle adapter installation, or physical database deletion
barrier was added. Step 12d is next; end-to-end PostgreSQL/Vitest lifecycle
coverage and the drain-before-delete barrier remain later work.

### Completed step: 12d — bounded asynchronous scheduler

`Arc<ConnectionWarmPool>::run_scheduler(host, port)` returns a caller-owned
future. It owns a `FuturesUnordered` of attempts and uses `reserve_next` to
fill available slots in round-robin order. Both its future count and the shared
semaphore use the effective limit: the minimum of configured concurrency,
global capacity, and Tokio's maximum semaphore permits. There is no task or
reservation backlog proportional to the number of databases.

The loop consumes completed results, fills available slots, and waits for an
attempt completion, a pool notification, a useful retry deadline, or pool
cancellation. Retry timers are considered only when an attempt slot, global
capacity, and the database's target permit more work. An overdue retry blocked
by capacity therefore parks until a state change. `establish` remains the sole
owner of failure accounting; the scheduler reports upstream failures without
advancing the backoff again.

An atomic guard rejects a second scheduler for the same pool. Returning or
dropping the future drops its attempts before releasing that guard, settling
reservations, sockets, and semaphore permits without detached tasks. Pool
cancellation stops the coordinator; a closed concurrency limiter or poisoned
state returns a terminal error. Disabled or already-cancelled pools return
without opening connections. Published idle sessions remain owned by the pool
until checkout, retirement, or the later shutdown drain.

Six connected socket tests cover round-robin order, registration and checkout
replenishment, bounded reservations across twenty databases, retirement and
stop cleanup, timer-driven retry and healthy-database progress, an overdue
retry waiting for global capacity, single-scheduler ownership and restart after
drop, and disabled/closed operation. Existing attempt tests retain coverage of
backoff reset, startup timeout, and late publication after retirement.

All seventy-two warm-pool tests pass. Step 12 is complete. Construction still
starts no work, and no listener or core lifecycle adapter invokes the scheduler
yet. Step 13 is next; runtime installation must retain the planned retirement,
drain-before-delete, and shutdown ownership requirements before enabling
warming end to end.

### Completed step: 13 — client checkout and cold fallback

`handle_connection` accepts an optional shared `Arc<ConnectionWarmPool>`.
Replication rejection, the `pgtest` control connection, protocol negotiation,
route validation, and successful lease attachment all precede checkout. The
handler uses the attached session's physical `DatabaseId` and the client's
original startup parameters to consume at most one matching spare.

Checkout executes inside the existing cancellation-first startup select. An
already-cancelled lease cannot consume a spare there. If no session is returned,
the handler immediately calls the existing cold connection path with the
assigned physical name and unchanged client parameters. It never waits for a
warm reservation, scheduler permit, or retry deadline. Upstream failure and
lease cancellation retain their existing client error codes.

Both paths transfer their `UpstreamSession` to the existing relay, preserving
its startup response bytes, socket, buffered client bytes, and lease guard.
The relay still sends startup once and observes lease cancellation. Consumed
sessions are never returned to the pool, and relay errors do not reconnect or
replay client traffic. Idle health detection remains step 14.

Five PostgreSQL integration tests in `connection/warm_tests.rs` exercise the
real manager, handler, pool, and relay. They verify:

- TCP and Unix clients receive the exact prepared backend PID and physical
  database, including the configured application name and search path.
- A later client uses a fresh backend, even while a replacement reservation is
  unfinished; session mutations do not carry over.
- Profile mismatch, a different physical database, and no configured pool take
  the cold path without consuming the matching spare. Cold startup preserves
  the requested application name and options.
- Invalid routes, replication, control traffic, and closed leases do not consume
  warm inventory.
- A query pipelined with Startup survives handoff, executes on the prepared
  backend, and receives its matching BackendKeyData with one AuthenticationOk.
  Lease release closes both warm and cold handed-off sessions.

Tests seed real upstream sessions explicitly to make hits deterministic. They
do not install the background scheduler or a lifecycle adapter. Existing TCP
and Unix listener entry points pass `None`; their public APIs and default cold
behavior remain unchanged. Runtime ownership and installation must be connected
with the later retirement/deletion and shutdown work before enabling warming.

Validation: all six connection tests (including the five new integration tests)
and all five existing TCP/Unix listener tests pass against the Docker PostgreSQL
harness. Formatting and whitespace checks pass. Step 13 is complete. Step 14
adds unused-socket health monitoring.

### Completed step: 14 — idle socket health

The existing scheduler now polls idle sockets alongside attempt completions,
state notifications, retry deadlines, and cancellation. `poll_idle_health`
registers its waker through a nonblocking one-byte read on each unused socket.
EOF, an I/O error, or any received byte evicts the spare, releases its capacity
once, and wakes normal fair replenishment. Pending reads park until readiness;
there is no periodic timer, health query, or extra task per connection.

The monitor conservatively discards unsolicited data instead of buffering or
decoding it. This includes benign notices and incomplete error frames. An unused
session with unexpected activity is replaced without affecting client traffic.
Connection attempts still use the existing concurrency limits and failure
backoff; idle eviction itself does not advance startup-failure backoff.

The nonblocking read poll runs under the pool state mutex to serialize it with
checkout and retirement. This narrowly extends the earlier step 7 guidance:
the health monitor may probe sockets under the lock, but must never await,
perform a blocking operation, send a query, or retain a socket reference beyond
that lock. Removed sockets are closed after unlocking. Once checkout removes a
session, even a stale monitor wakeup cannot read its client-owned traffic.

Checkout also probes the removed socket without waiting, so it can reject an
unhealthy spare before the scheduler notices it. It closes rejected sockets
outside the lock and tries remaining spares, bounded by the per-database target.
If none are usable, the handler immediately takes its existing cold path.
Healthy startup bytes and sockets transfer unchanged. Failure after this last
check remains possible; relay errors never trigger reconnection or query replay.

Four new connected tests verify:

- TCP/Unix checkout skips EOF and partial error data, preserves a healthy spare,
  releases capacity, and sends no health query.
- Socket activity alone triggers background replacement for EOF, partial
  ErrorResponse, and Notice data; retirement prevents another refill.
- An armed monitor cannot consume traffic from a checked-out socket, and
  retirement/scheduler cancellation preserve its client ownership.
- A PostgreSQL backend terminated with `pg_terminate_backend` is rejected by
  checkout and replaced through the real handler's cold path.

The earlier closed-lease regression now checks that rejected attachment leaves
the warm slot occupied, without expecting health-aware checkout to return a
backend that physical cleanup may already have terminated.

All 116 wire library tests pass against local sockets and Docker PostgreSQL.
Formatting and whitespace checks pass. Step 14 is complete; step 15 adds the
drain-before-delete barrier. CLI/server runtime installation remains pending.

### Completed step: 15 — drain before physical deletion

Core's `DatabaseLifecycle` now exposes `drain_database(DatabaseId)`, returning
an object-safe boxed, sendable future. Its default implementation succeeds
immediately, preserving behavior for the no-op hook and existing implementations.
The same lifecycle instance supplied before engine initialization is also
passed to `DatabaseCleanupWorker`.

Each cleanup job awaits the barrier before invoking `drop_database`. Waiting
happens in that job, outside the engine message loop and worker receiver loop,
so other leases, creation jobs, and cleanup jobs continue. A drain error is
reported through `CleanupFinished` without issuing DDL; core retains its
retirement record. Shutdown can cancel a pending wait without deleting the
database or reporting successful cleanup.

`ConnectionWarmPool` implements the lifecycle trait directly. Ready callbacks
register the physical identity, retirement callbacks disable further admission
and cancel attempts, and the async barrier idempotently enforces retirement
before waiting. A poisoned state lock returns `DatabaseDrainFailed` rather than
confirming a drain. Unregistered identities have no pool-owned resources and
complete immediately.

Each registered database owns a `TaskTracker`. Reservations acquire tokens under
the state lock; running attempts retain an additional token until their nested
connection future and concurrency permit have been dropped. Published idle
sessions carry their own tokens. The idle wrapper declares the session before
the token so socket disposal completes before the token is released. This also
covers sockets removed by health eviction or retirement but not yet disposed
outside the mutex. Rejected late publication closes its socket before releasing
the reservation's tracking.

Retirement closes the tracker under the admission lock. Draining then waits for
that closed tracker to become empty without holding the lock or competing for
the scheduler's notification. Multiple waiters are supported; cancelling one
wait does not reopen the database or cancel other waits. A successful checkout
ends warm ownership and releases its token outside the lock. Handed-off sessions
remain governed by lease cancellation and the relay, not the warm drain.

Retired entries remain as identity tombstones after draining. They retain no
warm sockets or attempts and prevent a duplicate ready callback from reopening
the same physical identity.

Six new regressions cover worker drain gating, drain failure skipping DDL,
shutdown during a wait, late publication, client handoff, the disposal window
after inventory removal, and poisoned state. The PostgreSQL integration scenario
installs the real pool hook before startup, warms a real backend, and holds both
a running and queued attempt during lease release. It verifies that the idle
backend closes while deletion waits, another lease can attach and be deleted,
and the original database is deleted only after both held attempts settle.

Validation: all 75 core and 120 wire library tests pass, including local socket
and Docker PostgreSQL coverage. Formatting and whitespace checks pass. Step 15
is complete. Step 16 adds full pool/background-work shutdown, including unused
ready databases; CLI/server runtime installation remains pending.

### Completed step: 16 — drain the entire pool during shutdown

`ConnectionWarmPool::shutdown()` cancels the root token, retires every registered
database, clears retry deadlines and scheduling order, and removes all idle
inventory. This includes ready databases that were never assigned to a lease.
Socket disposal happens outside the state mutex; per-database tracking remains
live until disposal completes. Queued and running attempts receive cancellation
and release their reservations, sockets, and concurrency permits.

The scheduler now holds its own `TaskTracker` token. Admission and tracker closure
use the pool state lock so shutdown cannot finish before a concurrently admitted
scheduler is accounted for. Its guard releases the token after its attempt
futures have dropped. Shutdown waits for scheduler exit and every database's
resource tracker before returning success. A scheduler future first polled after
shutdown returns without admitting new work.

The operation supports repeated calls, concurrent shutdown waiters, and concurrent
database drains. Cancelling a shutdown wait leaves admission closed and retirement
in effect; another call can resume waiting. The caller must keep polling its
scheduler and attempt futures, or drop them, while awaiting shutdown. The pool
does not detach tasks or forcibly abort caller-owned futures.

Shutdown closes warm resources without issuing database DDL. Sessions already
handed to clients remain owned by their leases and relays. A poisoned state lock
still triggers root cancellation and waits for scheduler exit, then returns
`WarmShutdownError::StatePoisoned` without claiming that inventory was drained.
Remaining owned sockets are disposed when the pool is dropped.

Six new regressions cover idle sockets plus queued/running attempts, simultaneous
waiters, cancellation of a shutdown wait, late publication and scheduler admission,
scheduler ownership without database resources, disabled pools, poisoned state,
and pending retry deadlines. A real PostgreSQL integration test installs the pool
before manager initialization, warms two databases without leasing either, then
verifies that shutdown closes both backends while preserving the databases.
Client handoff and release of background references are also checked.

Validation: all 126 wire library tests pass, including local socket and Docker
PostgreSQL coverage. Formatting and whitespace checks pass. Step 16 is complete.
Step 17 adds bounded initial warm-up and background-only startup; CLI/server
runtime installation remains pending.

### Completed step: 17 — bounded initial warm-up and runtime installation

`ConnectionWarmer` owns the optional pool and its scheduler task. Both the CLI and
environment-configured server construct it from validated settings, resolve the
default PostgreSQL user, and install its lifecycle hook before manager
initialization. Once initial databases exist, `start()` launches replenishment
and waits according to the startup policy. Disabled warming allocates neither a
pool nor a scheduler task and installs the no-op lifecycle hook.

For a positive startup wait, the pool snapshots the initial database identities
and targets `min(initial_database_count * per_database_target, max_total)` ready
sockets, with saturating multiplication. Only published idle sessions count;
reservations and connections still in startup do not. An empty initial population
completes immediately. Later database registrations do not extend this target.
The wait has one deadline for the whole initial population, independent of the
five-second per-attempt timeout. A deadline returns the current ready/target
counts, logs incomplete warm-up, and permits listening with cold fallback.
Pending attempts and retries continue in the background.

A zero startup wait returns `BackgroundOnly` immediately after launching the
scheduler. It does not wait for an attempt or idle session. Successful bounded
startup returns `Ready`; disabled mode, deadline expiry, and stopped warming have
distinct outcomes. Warm-up failure does not become an engine creation error.
An independent broadcast notification wakes startup observers without consuming
the scheduler's notification. Observers register before checking inventory to
avoid missing a concurrent publication.

The runtime exposes owned TCP and Unix listener methods that pass the same pool
to client handlers. Existing standalone listener functions retain their cold
behavior. Both applications wait for the startup outcome before opening their
listeners. Runtime database creation uses lifecycle notifications and background
replenishment without waiting for warm-up.

Normal application shutdown and listener-bind failures stop owned listeners,
drain the warmer, and then shut down the manager. The environment-configured
server now owns its TCP listener as well as its Unix listener. Interrupting
warm-up in the CLI or server uses the same cleanup path. Dropping the warmer
also closes admission, retires idle inventory, and aborts its owned scheduler;
explicit `shutdown()` waits for resource disposal. No scheduler task is detached
on cancellation of the initial wait.

Seven new regressions cover the global startup cap, shared TCP/Unix warm backend
handoff, background-only progress, deadline expiry with cold fallback, empty
initial populations followed by runtime creation, disabled allocation, interrupted
startup, partial readiness with attempts continuing after timeout, and concurrent
startup observers. The CLI subprocess suite additionally verifies that warm
backends exist before listener readiness is announced, a client receives an
initial backend, signals close sessions/listeners, and a Unix bind failure cleans
up an already-running TCP listener with warming enabled.

Validation: 133 wire library tests, 11 CLI and 10 server configuration tests, and
7 CLI subprocess tests pass (161 total), including Docker PostgreSQL and local
TCP/Unix sockets. Formatting and whitespace checks pass. The READMEs now describe
enabled warming, exact startup-profile matching, startup modes, and additional
backend-slot usage. Step 18 remains the broader integration/race validation;
performance comparison with the external Vitest suite remains step 19.

### Completed step: 18 — integration, isolation, and race validation

Four new runtime integration tests use the real lifecycle hook, scheduler,
PostgreSQL manager, and owned TCP/Unix listeners together. The concurrent cases
run on a four-thread Tokio runtime and release clients through a shared barrier.

- A twelve-client burst across two leases and both listener types consumes all
  four initial spares, gives every client a distinct backend, keeps each lease
  on its own database, and verifies table isolation. Pool accounting remains
  within the global, per-database, and scheduler concurrency limits at observed
  checkpoints; the existing scheduler tests exercise the limits directly.
- A used session leaves a temporary table, changed application name, prepared
  statement, and open transaction. The next connection to the same lease uses
  a different backend with clean session state and no uncommitted rows.
- Explicit release races eight connection attempts across both listeners while
  an existing client is active. Either admission outcome is accepted during
  the race, but every successful handoff must close, physical deletion must
  finish, and future requests for the released ID must return `55000`. Another
  lease remains usable.
- Lease expiry closes an active client and drains replenished idle spares before
  database deletion. Reusing the expired lease ID gets a different physical
  identity and database; dropping the old attachment preserves the new session.
  Expiry permits ID reuse by existing core design, whereas explicit release
  permanently closes the ID. The test advances the armed lease timer with
  Tokio's test clock, then resumes real time for socket and DDL work.

A new environment-server subprocess test runs both bounded-wait and
background-only startup. It verifies the configured warm profile, shared TCP/Unix
operation, distinct client backends, and initial backend reuse in bounded mode.
SIGINT must close active sessions, remove the Unix socket, stop TCP acceptance,
and leave no warm-profile backends. The server's test dependencies reuse packages
already in the workspace lockfile.

The complete coverage now spans these layers:

| Behavior | Coverage |
| --- | --- |
| Exact database/profile selection, mismatches, control and replication requests | Handler integration and checkout tests |
| One consumer per backend, clean session state, independent lease data | Concurrent runtime bursts and state-isolation integration |
| Capacity, fair replenishment, backoff, deadlines, cancellation | Scheduler and connected attempt tests |
| ErrorResponse, malformed/non-idle readiness, upstream death | Protocol, attempt, idle-health, and cold-fallback tests |
| Stale completion, retirement during warming/checkout, cleanup barriers | Publication, lifecycle, scheduler, and release-race tests |
| Lease expiry and physical identity on reuse | Runtime expiry integration and core generation tests |
| Shutdown, partial startup, listener failure, both listener types | Pool/runtime tests plus CLI and server subprocess suites |

Validation on macOS arm64 using `nightly-2026-09-10` and Docker PostgreSQL 18:

- Workspace tests: 245 passed; doctests: 2 passed and 1 previously ignored.
- Separate Rust example: build/default tests passed; its normally ignored
  `users_are_isolated_by_lease` correctness test was explicitly run and passed.
  The ignored latency benchmark was not run.
- Workspace and example formatting checks passed, as did whitespace checks.
- Workspace Clippy with all targets completed successfully, with warnings from
  the broad pedantic/restriction lint configuration (including test `unwrap`,
  indexing, naming, and style warnings). This is not a warning-free lint result.
- Workspace compilation with all targets and all features passed, including
  timing, allocation, and Prometheus profiling features. This was a compile
  check, not a profiled test run.

The full workspace command was `cargo test --offline --workspace`; it updated
only the server package's dependency list for the added test dependencies.
Subsequent lint, profiling, and example commands used `--locked --offline`.
The Rust example used the newly built CLI through `PGTEST_BIN`.

No production fix was required. Step 18 is complete; Linux execution and the
external Vitest suite were not run in this local validation. Step 19 compares
disabled, bounded-wait, and background-only performance with that external suite.
Warming remains opt-in; these correctness results establish no latency gain.

### Step 19 — user-run Docker comparison (measurements pending)

The user will run the existing external Vitest suite. Its inspected setup is
`/Users/said/Documents/demo-post-node-integresql`: `pnpm test` selects the server
image through `PGTEST_IMAGE`, creates a new server container per run, and saves
shutdown logs under `logs/`. The server reaches TimescaleDB through its shared
Unix socket volume; Vitest normally reaches the server through TCP. This setup
uses 32 initial databases and a CREATE connection pool of 60. Keep those settings,
PostgreSQL options, worker count, per-test connection-pool size, and transport
identical in every run.

The existing `.withEnvironment(...)` block does not forward warming variables
from the host process. Merely prefixing `pnpm test` with a
`PGTEST_CONNECTION_WARM_*` variable would therefore leave the container unchanged.
The comparison recipe stores the settings in each image's Docker `ENV`; the
inspected harness does not override those keys. No external repository edits are
needed.

#### Build once, outside the measured runs

From this repository:

```sh
just docker-build-warm-comparison step19
```

This builds one release server without Hotpath, then derives three images from
that exact base. Their binary and filesystem are identical; only warming settings
and image metadata differ.

| `PGTEST_IMAGE` | Count per database | Global cap | Attempt concurrency | Initial wait |
| --- | ---: | ---: | ---: | ---: |
| `pgtest-server:step19-disabled` | 0 | 32 | 4 | 5000 ms (ignored when disabled) |
| `pgtest-server:step19-bounded` | 1 | 32 | 4 | 5000 ms maximum |
| `pgtest-server:step19-background` | 1 | 32 | 4 | 0 ms |

All three set `PGTEST_CONNECTION_WARM_PARAMS={"client_encoding":"UTF8"}`.
Although `pg`'s `getStartupConf()` supplies only user and database by default,
the installed `pg-protocol` 1.16.0 serializer adds `client_encoding=UTF8` to the
actual startup packet. PgTest resolves the warm user from the container's
`PGTEST_PG_USER=metered`, and filters database out of matching. If URL options or
environment defaults add application name, timeouts, or options, include their
exact forwarded string values in the warm profile, consistently for all images.

The original comparison recipe incorrectly used `{}` after inspecting only
`getStartupConf()`. Both user-supplied modes subsequently reported zero successful
warm checkouts. The omitted encoding prevents every default `pg` client from
matching that profile. Those runs measure unused warming overhead, not the
benefit of successful warm handoffs. Rebuild all comparison images with the
corrected recipe before repeating the comparison. The serializer was exercised
locally with explicit fixture parameters to verify the actual wire packet;
successful handoffs in the external Docker workload still require a rerun.

The Hotpath build now reports the warm pool's state mutex under
`mutexes` with label `connection-warm-state`, including wait and hold timings.
Hotpath 0.25.1 has no semaphore wrapper, so the attempt semaphore is measured
with `measure_block!` and `future!`:

- `connection_warm::attempt_permit_wait`: elapsed acquisition time, including
  time suspended waiting for a permit (or until cancellation).
- `connection_warm::attempt_permit_hold`: elapsed time owning the permit,
  including connection startup and publication, ending on success, error, or
  cancellation.
- `connection_warm::attempt_permit_acquire`: future polling statistics. Poll
  duration alone does not measure semaphore wait time.

Rebuild the comparison images to include this instrumentation. It compiles away
when Hotpath is disabled. These measurements cover the application's warm pool;
SQLx's internal synchronization is not wrapped.

The Hotpath `debug` section also contains the gauge
`connection_warm::successful_checkouts`. Its `last_value` is the cumulative
number of healthy warm connections handed to clients, across all warm pools in
the process. Each successful checkout increments it once, after profile and
socket-health checks; misses and discarded sockets do not increment it. A warm
pool registers a zero value even if it never has a hit. With warming disabled
and no pool constructed, the entry is absent. This counts connection handoffs,
not distinct leases or successfully completed client sessions. Read `last_value`,
not `log_count` (which also counts zero-value registration updates).

`connection_warm::unused_closed_on_retirement` counts idle warm connections
closed without ever being handed to a client, during database retirement or
pool shutdown. Its `last_value` is cumulative across pools and counts sockets,
not retirement calls; repeated retirement/shutdown does not count them again.
It excludes failed or cancelled in-flight attempts, rejected late publications,
and sockets discarded by health checks. Like the checkout counter, it is
registered at zero when a pool is constructed. Use `last_value`, not `log_count`,
because one update can account for multiple closed connections.

Upstream startup rejection logs now retain PostgreSQL's SQLSTATE and primary
message, at both authentication and ReadyForQuery stages. Client fallback
failures also log the physical database name and ID. The earlier generic
`upstream rejected the startup request` warning from a non-clean run discarded
the upstream reason, so it cannot establish the cause of that failure. Preserve
the richer warning and PostgreSQL logs on the next occurrence; this diagnostics
change does not claim to fix the reliability issue.

#### Run the same suite in all three modes

From the external suite directory, using its usual supported Node/pnpm setup:

```sh
PGTEST_IMAGE=pgtest-server:step19-disabled /usr/bin/time -p pnpm test
PGTEST_IMAGE=pgtest-server:step19-bounded /usr/bin/time -p pnpm test
PGTEST_IMAGE=pgtest-server:step19-background /usr/bin/time -p pnpm test
```

Use the suite's managed-container path. An existing `PGTEST_DATABASE_URL` selects
an external server and bypasses `PGTEST_IMAGE`; that would not test these variants.
Keep existing Timescale reuse policy and all other environment settings fixed.
Run one unrecorded pass of each mode to remove first-use effects, then record
three rounds in this order:

| Round | First | Second | Third |
| --- | --- | --- | --- |
| 1 | disabled | bounded | background |
| 2 | background | disabled | bounded |
| 3 | bounded | background | disabled |

Run them sequentially. Preserve the raw Vitest summaries and the corresponding
saved server logs. `/usr/bin/time`'s `real` is the whole command duration, including
container setup, migrations, PgTest startup, and teardown; label it accordingly.
Keep Vitest's own duration and phase breakdown separate. Neither is a standalone
PgTest startup measurement. Record startup duration separately only if an
existing timer measures that boundary. Image building is excluded.

#### Collect diagnostic profiles separately

Build the same matrix with profiling enabled:

```sh
just docker-build-warm-comparison step19-hotpath hotpath
```

Then run one pass per mode from the suite directory:

```sh
PGTEST_IMAGE=pgtest-server:step19-hotpath-disabled pnpm test
PGTEST_IMAGE=pgtest-server:step19-hotpath-bounded pnpm test
PGTEST_IMAGE=pgtest-server:step19-hotpath-background pnpm test
```

The existing harness already sets `HOTPATH_LIMIT=0` and
`HOTPATH_OUTPUT_FORMAT=json`, stops the server with SIGINT, and saves shutdown
output. Send those three reports with the uninstrumented timing results. Keep
profiled durations in a separate group; they are diagnostic runs, not additional
samples of uninstrumented performance.

#### Results to send back

For each recorded run, send the mode, pass/fail and test counts, Vitest summary
(including its phase durations), whole-command `real` time, and saved log filename.
Also include fixed worker count, `TEST_PG_POOL_MAX`, Timescale reuse setting,
Docker CPU/memory allocation, architecture, and any deviations from the settings
above. Retain warnings about incomplete warm-up, upstream failures, or connection
limits; a failed run is not a valid speedup sample.

| Mode | Round | Tests passed | Vitest duration | Whole command `real` | Startup if measured | Server log |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| disabled | 1–3 | pending | pending | pending | unavailable/pending | pending |
| bounded | 1–3 | pending | pending | pending | unavailable/pending | pending |
| background | 1–3 | pending | pending | pending | unavailable/pending | pending |

Include client connection samples/percentiles, warm hits/misses, and backend-count
samples if the suite already records them. The server's profiled build now emits
successful warm checkouts as described above and server-side startup timings as
described below. End-to-end client latency and backend-count samples still need
client/PostgreSQL measurements; mark them unavailable when absent.
Hotpath's upstream-connect totals include both
warm attempts and cold connections; they cannot establish hit rate, client
p50/p95/p99, or saved wall time. Relay duration includes session lifetime and is
not connection latency. Do not pool or average per-run percentiles as though they
were raw samples.

Compare the median and range across the three runs of each mode, using identical
duration boundaries. Assess whole-run cost alongside any available client latency
and backend usage. Use profiles to explain where work moved and whether creation,
cleanup, or upstream startup got slower; nested/concurrent timings are not
additive. Keep warming disabled by default until repeated representative results
justify a different choice.

Preparation validation: Just renders the recipe and its generated Bash passes
syntax checking. Docker images and suite runs are intentionally left to the user;
no new measurements or speedup claims have been made. Step 19 stays unchecked
until the supplied results have been compared and any missing measurements have
been explicitly recorded as limitations.

### Scheduling policy after the first profiles

One supplied profile recorded 309 successful warm checkouts and 246 unused
connections closed on retirement. To avoid recreating spares after each handoff,
the scheduler now reserves work only when:

```text
successful handoffs + idle spares + in-flight attempts < connection-warm-count
```

Each successful checkout increments its database's handoff count under the same
mutex that removes the spare. The checkout health probe is a single nonblocking
read under that mutex; socket disposal remains outside it. This prevents a racing
scheduler from reserving a replacement between removal and handoff. Health
rejections and failed attempts do not consume the handoff budget.

The global cap continues to count only idle spares and in-flight attempts. A
checkout frees global capacity for other databases or for unfilled slots within
the same database's remaining budget. Duplicate registration cannot reset the
budget; a new physical `DatabaseId` receives a new budget. Cold fallback, startup
waiting, retry backoff, retirement, and shutdown retain their existing behavior.

This policy applies to both bounded-startup and background-only warming and needs
no additional setting. Rebuild the comparison images before collecting new
profiles. The existing successful-checkout and unused-on-retirement gauges remain
available; performance and non-clean-run reliability still require measurement.

### Separating warm and cold startup in Hotpath

Timing-enabled builds emit these additional function/block measurements:

| Label | Measured interval |
| --- | --- |
| `connection::client_startup_to_ready` | After a normal application Startup message has been decoded and routed, through protocol negotiation, frontend AuthenticationOk, lease attachment, upstream selection/startup, and writing the saved responses through ReadyForQuery. |
| `connection::warm_checkout` | The synchronous pool lookup and health check, including misses. |
| `connection::warm_startup_to_ready` | After a successful warm checkout, through forwarding any pipelined client bytes and writing the cached startup responses through ReadyForQuery. |
| `connection::cold_startup_to_ready` | After a warm miss (or with warming disabled), through opening/authenticating the upstream and the same response forwarding. |
| `connection::cold_upstream_startup` | Foreground socket connection, authentication, and receipt of upstream ReadyForQuery. Nested within the cold startup interval. |
| `connection_warm::upstream_startup` | Background socket connection, authentication, and receipt of upstream ReadyForQuery. Excludes waiting for a warm-attempt permit. |

Compare the warm/cold tail averages and percentiles to see whether avoiding an
upstream handshake reduces startup time. Both tails exclude the preceding
frontend negotiation, lease attachment, and pool lookup; the common
`client_startup_to_ready` measurement includes those stages, but aggregates both
paths. It excludes frontend socket establishment, SSL negotiation, and waiting
for the client's Startup packet. Completion means the server finished its write,
not that the client has received or processed it.

Control connections to `pgtest` do not enter these startup measurements. As with
other Hotpath timing scopes, failed or cancelled attempts are recorded up to
exit/drop too; timing call counts alone are not successful-connection counts.
Use runs without connection failures when comparing successful startup latency.

The existing `postgres_upstream::connect` and `authenticate` rows still aggregate
both foreground and background calls. The new upstream rows show where that work
happens; do not add nested timings or subtract percentiles. `session_relay::run`
and its I/O counters now begin after startup response forwarding, so they describe
the subsequent session rather than its initialization.
