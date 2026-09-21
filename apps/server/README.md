# pgtest server

Contains the pgtest server executable, including environment configuration,
logging, startup of TCP and optional Unix socket listeners, and shutdown handling.

`PGTEST_LISTEN_ADDR` selects the TCP bind IP address (default `127.0.0.1`), and
`PGTEST_LISTEN_PORT` selects its port (default `6432`). Use an IP without a port,
such as `0.0.0.0` or `::1`. Port `0` selects an available port. Docker defaults to
`0.0.0.0` so published ports are reachable.

Set `PGTEST_UNIX_SOCKET_DIR` to an existing directory to enable the Unix listener.
`PGTEST_UNIX_SOCKET_PORT` selects its `.s.PGSQL.<port>` filename and defaults to
`6432`, independently of `PGTEST_LISTEN_PORT` and the upstream `PGTEST_PG_PORT`.
Valid values are `1`–`65535`; setting the port alone does not enable the listener.
