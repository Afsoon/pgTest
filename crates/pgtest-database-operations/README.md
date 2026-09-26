# pgtest-database-operations

Contains PostgreSQL configuration, database creation and deletion operations,
database naming helpers, and test-container support for PostgreSQL tests.

With `hotpath` enabled, `tokio-postgres` calls appear in Hotpath's SQL report.
This covers database listing, server-version queries, creation, and individual
drops, including pipelined cleanup and failed executions. Timings measure the
driver future after pool checkout, so they include network and PostgreSQL queue
time but exclude pool acquisition. Cancelled futures do not
emit a completion event.

DDL labels replace database and template identifiers with placeholders to group
leases into stable query buckets. Bound parameter values are not recorded.
The adapter emits `sqlx::query` completion events with `db.driver=tokio-postgres`
for Hotpath 0.25's `sqlx_tracing_layer`; this does not use the SQLx driver. The
CLI and server already install that collector with console filtering kept
separate. With profiling disabled, the adapter does not read the clock or emit
events.
Profiling builds box each driver future to keep the startup stack bounded; that
allocation is visible in allocation profiles. Builds without profiling return
the original driver future directly.
