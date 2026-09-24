# Ready PostgreSQL connections: guided implementation

Status: design agreed; baseline test added; external Vitest suite selected for
remaining performance measurements. Pool registration, reservations, publication,
and checkout are complete. Core lifecycle integration is next; runtime warming
is not wired yet.

## Working agreement

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

- `crates/pgtest-wire/src/connection.rs` assigns a lease, then opens an upstream
  connection with the client's startup parameters.
- `crates/pgtest-wire/src/postgres_upstream.rs` produces an `UpstreamSession`
  containing a socket and startup response bytes. Startup now rejects
  ErrorResponse and requires an exactly sized, idle ReadyForQuery frame.
- `crates/pgtest-wire/src/session_relay.rs` forwards the startup response bytes
  and relays traffic until completion or lease cancellation.
- Core creates initial databases during engine initialization and additional
  databases through creation workers. Both paths need lifecycle notifications.
- `LeaseSession` currently carries the database name and lease cancellation;
  warming also needs the physical `DatabaseId`.

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
without waiting for replenishment. Replenish eligible databases in the background.
Monitor unused sockets for closure/errors, without a health-query round trip on
checkout. Failures can still race with handoff; never replay queries once client
traffic has been forwarded.

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
global capacity and concurrency. A per-database target larger than available
global capacity is best-effort, constrained by that capacity. Other client
profiles connect normally.

Count idle sockets and in-flight warm attempts against the global warm cap;
active client sessions do not count against it. Use fair replenishment across
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
- [ ] 8. Add the no-op core lifecycle interface and DatabaseId in LeaseSession.
- [ ] 9. Notify successful initial and runtime database creation.
- [ ] 10. Notify retirement before cleanup is submitted.
- [ ] 11. Establish warm sessions with timeouts, concurrency limits, and cancellation.
- [ ] 12. Add bounded, fair replenishment and failure backoff.
- [ ] 13. Integrate checkout and cold fallback with client connection handling.
- [ ] 14. Monitor unused-socket health and replace dead spares.
- [ ] 15. Drain unused sockets and warm attempts before database deletion.
- [ ] 16. Drain pool resources and background work during shutdown.
- [ ] 17. Add bounded initial warm-up and background-only startup mode.
- [ ] 18. Validate integration, isolation, races, failures, and both listener types.
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
