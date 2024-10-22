Redis Scanner
=============

Utilities for inspecting or operating on Redis servers via the `SCAN` interface. This module is intended to be used in
cases where other tools may not work due to memory limits or certain access control restrictions.

All commands are read-only and do not read values, with the following exceptions:

* `expire` sets a TTL on keys and therefore is not read-only.
* `touch` modifies a key's last access time and therefore is not read-only.
* `sum` reads integer values and can conditionally reset them to 0.

## Commands

```
Utilities for inspecting a Redis keyspace via the SCAN command.

Usage: redis_scanner [OPTIONS] <COMMAND>

Commands:
  idle    Inspect keys via the `OBJECT IDLETIME` command
  memory  Inspect keys via the `MEMORY USAGE` command
  touch   Call `TOUCH` on each key
  ttl     Inspect keys via the `TTL` command
  expire  Set an expiration on keys
  sum     Read and sum counters via the `GET` or `GETSET` command
  help    Print this message or the help of the given subcommand(s)

Options:
  -H, --host <STRING>
          The server hostname
          [default: 127.0.0.1]
  -p, --port <NUMBER>
          The server port
          [default: 6379]
      --db <NUMBER>
          The database to `SELECT` after connecting
  -u, --username <STRING>
          The username to provide when authenticating
          [env: REDIS_USERNAME=]
  -P, --password <STRING>
          The password to provide after connection
          [env: REDIS_PASSWORD=]
      --sentinel-service <STRING>
          The name of the sentinel service, if using a sentinel deployment
  -c, --cluster
          Whether to discover other nodes in a Redis cluster
  -r, --replicas
          Whether to scan replicas rather than primary nodes. This also implies `--cluster`
  -q, --quiet
          Whether to hide progress bars and messages before the final output
  -i, --ignore
          Ignore errors, if possible
  -R, --reconnect <NUMBER>
          An optional reconnection delay. If not provided the client will stop scanning after any disconnection
      --tls
          Whether to use TLS when connecting to servers
      --tls-key <PATH>
          A file path to the private key for a x509 identity used by the client
      --tls-cert <PATH>
          A file path to the certificate for a x509 identity used by the client
      --tls-ca-cert <PATH>
          A file path to a trusted certificate bundle
      --pattern <STRING>
          The glob pattern to provide in each `SCAN` command
          [default: *]
      --page-size <PAGE_SIZE>
          The number of results to request in each `SCAN` command
          [default: 100]
  -d, --delay <DELAY>
          A delay, in milliseconds, to wait between `SCAN` commands
          [default: 0]
  -f, --filter <REGEX>
          A regular expression used to filter keys while scanning. Keys that do not match will be skipped before any subsequent operations are performed
  -s, --reject <REGEX>
          A regular expression used to reject or skip keys while scanning. Keys that match will be skipped before any subsequent operations are performed
      --min-refresh-delay <NUMBER>
          Set a minimum refresh delay between progress bar updates, in milliseconds.
  -h, --help
          Print help (see a summary with '-h')
  -V, --version
          Print version
```

### Idle

```
Inspect keys via the `OBJECT IDLETIME` command

Usage: redis-scanner idle [OPTIONS]

Options:
  -f, --format <STRING>          The output format, if applicable [default: table] [possible values: table, csv, json]
  -S, --sort <STRING>            The sort order to use [default: desc] [possible values: asc, desc]
      --max-index-size <NUMBER>  The number of records to index in memory while scanning. Default is `--limit + --offset`
  -l, --limit <NUMBER>           The maximum number of results to return [default: 100]
  -o, --offset <NUMBER>          The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in memory [default: 0]
  -F, --file <PATH>              Write the final output to the provided file
  -h, --help                     Print help
  -V, --version                  Print version
```

### TTL

```
Inspect keys via the `TTL` command

Usage: redis-scanner ttl [OPTIONS]

Options:
  -f, --format <STRING>          The output format, if applicable [default: table] [possible values: table, csv, json]
  -S, --sort <STRING>            The sort order to use [default: desc] [possible values: asc, desc]
      --max-index-size <NUMBER>  The number of records to index in memory while scanning. Default is `--limit + --offset`
  -l, --limit <NUMBER>           The maximum number of results to return [default: 100]
  -o, --offset <NUMBER>          The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in memory [default: 0]
      --skip-missing             Skip keys that do not have a TTL. Missing TTLs act as `-1` for sorting purposes
  -F, --file <PATH>              Write the final output to the provided file
  -h, --help                     Print help
  -V, --version                  Print version
```

### Touch

```
Call `TOUCH` on each key

Usage: redis-scanner touch

Options:
  -h, --help     Print help (see more with '--help')
  -V, --version  Print version
```

### Memory Usage

```
Inspect keys via the `MEMORY USAGE` command

Usage: redis-scanner memory [OPTIONS]

Options:
  -f, --format <STRING>              The output format, if applicable [default: table] [possible values: table, csv, json]
  -S, --sort <STRING>                The sort order to use [default: desc] [possible values: asc, desc]
  -g, --group-by <REGEX>             A regular expression used to group or transform keys (via `Regex::captures`) while aggregating results. This is often used to extract substrings in a key
      --group-by-delimiter <STRING>  A delimiter used to `slice::join` multiple values from `--group-by`, if applicable [default: :]
      --filter-missing-groups        Whether to skip keys that do not capture anything from the `--group-by` regex
  -l, --limit <NUMBER>               The maximum number of results to return [default: 100]
  -o, --offset <NUMBER>              The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in memory [default: 0]
      --max-index-size <NUMBER>      The number of records to index in memory while scanning. Default is `--limit + --offset`
  -s, --samples <NUMBER>             The number of samples to provide in each `MEMORY USAGE` command
  -F, --file <PATH>                  Write the final output to the provided file
  -h, --help                         Print help
  -V, --version                      Print version
```

This subcommand is often the most complex. It includes an additional `group-by` interface that callers can use to
combine `MEMORY USAGE` results across keys, even across cluster nodes. This is done via
the [regex captures](https://docs.rs/regex/latest/regex/struct.Regex.html#method.captures) interface.

This is best explained with an example. Consider a sessions and user cache scenario with the following key formats:

* `sessions:<user id>` - Some session blob.
* `users:<user id>` - Some cached user data.

And 4 keys:

* `sessions:123` -> `foo`
* `sessions:234` -> `foo`
* `users:123` -> `foo`
* `users:234` -> `foo`

To see memory usage by key:

```
$ redis_scanner memory
+--------------+--------+
| Key          | Memory |
+--------------+--------+
| sessions:123 | 72     |
+--------------+--------+
| users:123    | 72     |
+--------------+--------+
| sessions:234 | 72     |
+--------------+--------+
| users:234    | 72     |
+--------------+--------+
```

To see memory usage by user ID across both prefixes:

```
$ redis_scanner memory --group-by "^\w+:(\d*)"
+-----+--------+
| Key | Memory |
+-----+--------+
| 123 | 144    |
+-----+--------+
| 234 | 144    |
+-----+--------+
```

To see memory usage by prefix:

```
$ redis_scanner memory --group-by "^(\w+):\d*"
+----------+--------+
| Key      | Memory |
+----------+--------+
| sessions | 144    |
+----------+--------+
| users    | 144    |
+----------+--------+
```

### Sum

```
Read and sum counters via the `GET` or `GETSET` command

Usage: redis_scanner sum [OPTIONS]

Options:
  -f, --format <STRING>              The output format, if applicable [default: table] [possible values: table, csv, json]
  -S, --sort <STRING>                The sort order to use [default: desc] [possible values: asc, desc]
  -g, --group-by <REGEX>             A regular expression used to group or transform keys (via `Regex::captures`) while aggregating results. This is often used to extract substrings in a key
      --group-by-delimiter <STRING>  A delimiter used to `slice::join` multiple values from `--group-by`, if applicable [default: :]
      --filter-missing-groups        Whether to skip keys that do not capture anything from the `--group-by` regex
  -l, --limit <NUMBER>               The maximum number of results to return. Set to 0 to read the entire data set [default: 100]
  -o, --offset <NUMBER>              The number of results to skip, after sorting. Note: the client must hold at least `limit + offset` keys in memory [default: 0]
      --max-index-size <NUMBER>      The number of records to index in memory while scanning. Default is `--limit + --offset`
  -F, --file <PATH>                  Write the final output to the provided file
      --decr                         Whether to decrement counters by the value read, while scanning
  -h, --help                         Print help
  -V, --version                      Print version
```

See the `memory` command for more info on the `--group-by` and `--group-by-filter` argv.

## TLS Notes

* Both `native-tls` and `rustls` expect a PKCS#8 key
* `rustls` expects DER-encoded certs and keys
* `native-tls` expects PEM-encoded certs and keys
* `redis-cli` expects a PKCS#1 key