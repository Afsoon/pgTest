# pgtest-wire

Handles PostgreSQL client connections over TCP and Unix sockets and supports
pgtest control commands. Application connections use `template/lease-id` as the
database name; pgtest-wire routes each connection to the PostgreSQL database
assigned to that lease and forwards its SQL there.

## Accepted SQL commands

Connect to the virtual `pgtest` database to run these control commands:

| SQL | Result |
| --- | --- |
| `SELECT 1;` | Returns the integer `1`. |
| `SELECT pgtest_release('test-42');` | Releases the lease and returns `true` on success. |
| `SELECT pgtest_release($1);` | Releases the lease supplied as a bound text parameter and returns `true` on success. |
| `SELECT pgtest_release($1::text);` | Same, with an explicit text cast. |

Bound parameters require the extended query protocol, as used by client prepared
statements. Keywords and the function name are case-insensitive, whitespace is
allowed between tokens, and the trailing semicolon is optional. Escape single
quotes inside a lease ID by doubling them, for example `'suite''s test'`.

Other SQL and multiple statements in one query are unsupported on the control
database. Application connections forward SQL to their leased PostgreSQL database.
