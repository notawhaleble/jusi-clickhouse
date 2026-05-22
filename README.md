# jusi-clickhouse

ClickHouse provider plugin for the Jusi SQL family.

Configure a SQL target in the Jusi session config:

```toml
[sql.analytics]
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

For a local manual-test database:

```bash
./scripts/run-clickhouse.sh
```

The script builds `jusi-clickhouse-dev`, starts a local ClickHouse container, loads demo tables under `demo`, waits until it is ready, and prints a matching `jusi.toml` block.
