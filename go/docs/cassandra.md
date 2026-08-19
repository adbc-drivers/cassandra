---
# Copyright (c) 2025-2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{}
---

{{ cross_reference|safe }}
# Cassandra Driver {{ version }}

{{ heading|safe }}

This is an implementation of an [ADBC](https://arrow.apache.org/adbc/) driver for [Apache Cassandra](https://cassandra.apache.org/).

## Installation

```bash
go get github.com/adbc-drivers/cassandra/go
```

## Usage

```go
import (
    "context"

    "github.com/adbc-drivers/cassandra/go"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    ctx := context.Background()

    // Create driver
    drv := cassandra.NewDriver(memory.DefaultAllocator)

    // Open database
    db, err := drv.NewDatabaseWithContext(ctx, map[string]string{
        "cassandra.hosts": "127.0.0.1",
        "cassandra.keyspace": "my_keyspace",
        "username": "cassandra",
        "password": "cassandra",
    })
    if err != nil {
        panic(err)
    }
    defer db.Close(ctx)

    // Open connection
    conn, err := db.Open(ctx)
    if err != nil {
        panic(err)
    }
    defer conn.Close(ctx)

    // Create statement
    stmt, err := conn.NewStatement(ctx)
    if err != nil {
        panic(err)
    }
    defer stmt.Close(ctx)

    // Execute query
    err = stmt.SetSqlQuery(ctx, "SELECT * FROM my_table")
    if err != nil {
        panic(err)
    }

    reader, _, err := stmt.ExecuteQuery(ctx)
    if err != nil {
        panic(err)
    }
    defer reader.Release()

    // Process results
    for reader.Next() {
        record := reader.RecordBatch()
        // Process record...
    }
}
```

## Configuration Options

### Connection Options

- `cassandra.hosts` - Comma-separated list of Cassandra hosts (default: "127.0.0.1")
- `cassandra.keyspace` - Keyspace to connect to
- `cassandra.port` - Port number (default: "9042")

### Authentication

- `username` / `cassandra.auth.username` - Username for authentication
- `password` / `cassandra.auth.password` - Password for authentication

### Connection Pool

- `cassandra.num_conns` - Number of connections per host (default: 2)
- `cassandra.page_size` - Page size for query results (default: 5000)
- `cassandra.consistency` - Consistency level (default: "LOCAL_QUORUM")

### Timeouts

- `cassandra.connect_timeout` - Connect timeout in milliseconds (default: 10000)
- `cassandra.timeout` - Query timeout in milliseconds (default: 10000)

### TLS/SSL

- `cassandra.enable_tls` - Enable TLS (default: disabled)
- `cassandra.tls.cert_path` - Path to client certificate
- `cassandra.tls.key_path` - Path to client key
- `cassandra.tls.ca_path` - Path to CA certificate
- `cassandra.tls.skip_verify` - Skip certificate verification
- `cassandra.tls.hostname_override` - Override hostname for certificate verification

### Protocol

- `cassandra.protocol_version` - CQL protocol version (default: 4)

## URI Format

You can also use a URI to configure the connection:

```
cassandra://[username:password@]host[:port][/keyspace][?options]
```

Example:
```
cassandra://user:pass@localhost:9042/my_keyspace?page_size=1000&consistency=ONE&timeout=5000
```

Supported query parameters are `num_conns`, `page_size`, `consistency`,
`connect_timeout`, `timeout`, `enable_tls`, `tls_cert_path`, `tls_key_path`,
`tls_ca_path`, `tls_skip_verify`, `tls_hostname_override`, and
`protocol_version`. Boolean values must be `true` or `false`, and values such as
file paths must be URL-encoded. Unknown or repeated parameters are rejected.
Explicit ADBC options override values from the URI.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

{{ compatibility_info|safe }}

{{ footnotes|safe }}
