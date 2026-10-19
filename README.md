# jusi-clickhouse

`jusi-clickhouse` is the ClickHouse exact-provider plugin for Jusi 1.0 and the
shared `jusi-sql` family. It exposes the `clickhouse` provider for `%%sql`
cells. The family owns target selection, magic dispatch, completion ranges,
metadata caching, operation routing, and common VisiData SQL commands.

Configure a SQL target in the Jusi session config:

```toml
[sql.targets.analytics]
provider = "clickhouse"
host = "localhost"
port = 8123
username = "default"
password = ""
database = "default"
initial_fetch = 100
```

Use it from a cell:

```sql
%%sql analytics
SELECT *
FROM system.numbers
LIMIT 1000
```

Results open in VisiData. Press `1`-`9` to fetch more rows, `gf` to prompt for a fetch count (`0` fetches the rest), and `gb` to open the selected raw value through VisiData. `gc` and `gr` are present for SQL-family parity, but ClickHouse transaction control is reported as unsupported.

Follow-up cells reuse the same VisiData application, ClickHouse connection, and
open result streams. `JusiInterrupt` requests cancellation of active queries.
Closing the client closes its streams, connection, private control socket, and
staged launch data.

The terminal application loads normal VisiData user configuration and plugins
before installing Jusi and ClickHouse commands. This includes `~/.visidatarc`
and paths selected through `VD_CONFIG` and `VD_DIR`.

For a local manual-test database:

```bash
./scripts/run-clickhouse.sh
```

The script builds `jusi-clickhouse-dev`, starts a local ClickHouse container, loads demo tables under `demo`, waits until it is ready, and prints a matching `jusi.toml` block.

## Initialize without running a query

Execute a cell containing only `%%sql analytics`, or run `:JusiBootstrap` while
keeping a query draft in the cell. This opens a ready client and starts its
normal metadata warm-up without running the draft or a dummy SQL query.
Completion can use cached metadata immediately and fresh metadata as it arrives.
Subsequent queries use the same client. Repeated `:JusiBootstrap` reuses it;
changing the target alias requires closing the old client first.

This requires the bootstrap-capable core and matching `jusi-sql` and SQL provider
updates. No query history entry is added for initialization.
