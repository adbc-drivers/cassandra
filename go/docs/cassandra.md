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

This driver provides access to [Apache Cassandra][cassandra], a distributed,
open-source NoSQL database.

## Installation

The Cassandra driver can be installed with
[dbc](https://docs.columnar.tech/dbc):

```bash
dbc install --pre cassandra
```

## Connecting

To connect, provide a Cassandra connection string as the `uri` option:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
    driver="cassandra",
    db_kwargs={
        "uri": "cassandra://localhost:9042/my_keyspace",
    },
)
```

The example uses Python and the
[adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package,
but the same options apply through other ADBC driver managers. See
[adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts) for
end-to-end examples.

### Connection String Format

```text
cassandra://[username[:password]@][host[:port]][/keyspace][?parameter1=value1&parameter2=value2...]
```

Components:

- `scheme`: `cassandra://` (required)
- `username`: Username for authentication (optional)
- `password`: Password for authentication (optional; requires a username)
- `host`: Cassandra contact point (optional; defaults to `127.0.0.1`)
- `port`: Native transport port (optional; defaults to `9042`)
- `keyspace`: Initial keyspace (optional)
- Query parameters: Connection options listed below

Examples:

- `cassandra://localhost:9042/my_keyspace`
- `cassandra://user:password@cassandra.example.com/my_keyspace`
- `cassandra://localhost/my_keyspace?page_size=1000&consistency=ONE&timeout=5000`
- `cassandra:///my_keyspace?enable_tls=true&tls_ca_path=%2Fpath%2Fto%2Fca.pem`

The URI accepts one contact point. To supply multiple contact points, use the
`cassandra.hosts` option instead. Query parameter values must be URL-encoded
when necessary. Unknown or repeated query parameters are rejected. Options
specified separately from the URI take precedence over URI values.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

#### Python floating-point collections

The Python driver manager infers a standard Python list of floating-point
values as an Arrow `list<double>`. The Cassandra driver converts this input to
`float32` when binding a CQL `vector<float, N>`, but it does not currently
perform the same conversion for CQL `list<float>` or `set<float>` columns.

To bind a Python value to `list<float>` or `set<float>`, provide Arrow data
whose list elements are explicitly typed as `float32`, for example with
`pyarrow.list_(pyarrow.float32())`. Otherwise, gocql rejects the inferred
`float64` elements.

## Options

### Connection Options

`uri`
: **Type:** string. **Default:** not set.

  Cassandra connection string in the format described above. If it is not
  set, the driver uses the defaults of `127.0.0.1` and port `9042`.

`username` and `password`
: **Type:** string. **Default:** not set.

  Standard ADBC options for username/password authentication. The aliases
  `cassandra.auth.username` and `cassandra.auth.password` are also supported.

`cassandra.hosts`
: **Type:** string. **Default:** `127.0.0.1`.

  Comma-separated list of Cassandra contact points. All contact points use the
  port set by `cassandra.port`.

`cassandra.port`
: **Type:** integer. **Default:** `9042`.

  Native transport port used for all contact points.

`cassandra.keyspace`
: **Type:** string. **Default:** not set.

  Initial keyspace for the connection.

`cassandra.num_conns` (URI query parameter: `num_conns`)
: **Type:** integer. **Default:** `2`.

  Number of connections to create per host.

`cassandra.page_size` (URI query parameter: `page_size`)
: **Type:** integer. **Default:** `5000`.

  Maximum number of rows requested in each page of query results.

`cassandra.consistency` (URI query parameter: `consistency`)
: **Values:** `ANY`, `ONE`, `TWO`, `THREE`, `QUORUM`, `ALL`, `LOCAL_QUORUM`,
  `EACH_QUORUM`, or `LOCAL_ONE`. **Default:** `LOCAL_QUORUM`.

  Cassandra consistency level to use for queries. Values are
  case-insensitive.

`cassandra.connect_timeout` (URI query parameter: `connect_timeout`)
: **Type:** integer. **Default:** `10000`.

  Connection timeout in milliseconds.

`cassandra.timeout` (URI query parameter: `timeout`)
: **Type:** integer. **Default:** `10000`.

  Query timeout in milliseconds.

`cassandra.protocol_version` (URI query parameter: `protocol_version`)
: **Type:** integer. **Default:** `4`.

  CQL native protocol version.

### TLS Options

Boolean URI query parameters must be `true` or `false`.

`cassandra.enable_tls` (URI query parameter: `enable_tls`)
: **Type:** boolean. **Default:** `false`.

  Enable TLS for the connection.

`cassandra.tls.ca_path` (URI query parameter: `tls_ca_path`)
: **Type:** string. **Default:** not set.

  Path to a PEM-encoded CA certificate used to verify the server certificate.

`cassandra.tls.cert_path` and `cassandra.tls.key_path` (URI query parameters:
`tls_cert_path` and `tls_key_path`)
: **Type:** string. **Default:** not set.

  Paths to a PEM-encoded client certificate and private key. Set both options
  to configure mutual TLS.

`cassandra.tls.hostname_override` (URI query parameter:
`tls_hostname_override`)
: **Type:** string. **Default:** not set.

  Server name used for certificate verification.

`cassandra.tls.skip_verify` (URI query parameter: `tls_skip_verify`)
: **Type:** boolean. **Default:** `false`.

  Disable server certificate verification. This is not recommended for
  production use.

## Compatibility

{{ compatibility_info|safe }}

{{ footnotes|safe }}

[cassandra]: https://cassandra.apache.org/
