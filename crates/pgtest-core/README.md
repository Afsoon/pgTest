# pgtest-core

Manages test database leases, keeps databases available for tests, and coordinates
database creation and cleanup.

Creation requests carry a count and the first reserved database ID. The worker
dispatches each batch immediately and runs CREATEs across the creation pool,
with one outstanding command per checked-out connection. Each batch limits its
pending operations to the configured creation pool size, and concurrent batches
share that pool's execution limit. Each operation is attempted once, and results
are reported separately, including acquisition and execution failures.

Startup uses the same bounded creation path for its initial count and waits for all
results. A partial failure fails startup after draining the batch; successful
databases remain for the next startup cleanup sweep.

The cleanup worker buffers requests and dispatches them every 100 ms. Each tick
moves the pending batch into a tracked Tokio task, allowing the worker to keep
accepting requests while earlier batches run. The cleanup pool limits concurrent
connections. Each drop is attempted once, with success or failure reported per
database. SQL operations do not retry or wait for backoff.

Closing the request channel flushes buffered work immediately. Explicit shutdown
discards buffered requests and cancels active tasks. Already submitted PostgreSQL
commands may still finish after cancellation.
