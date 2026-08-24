<!--
  Copyright (c) 2026 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ADBC Driver for Cassandra

An [ADBC driver](https://arrow.apache.org/adbc/) for
[Apache Cassandra](https://cassandra.apache.org/), built on the
[Apache Cassandra GoCQL Driver](https://github.com/apache/cassandra-gocql-driver).

## Installation

Pre-packaged builds are available for various platforms from the
[Columnar](https://columnar.tech) CDN. They can be installed by any tool that
supports [ADBC](https://arrow.apache.org/adbc/) Driver Manifests, such as
[dbc](https://columnar.tech/dbc):

```sh
dbc install --pre cassandra
```

Only prerelease versions of the driver are currently available, so `--pre` is
required.

See [Building](#building) if you would rather build the driver yourself.

## Usage

The driver accepts the following URI forms via the `uri` database option:

- `cassandra://host:9042/keyspace`
- `cassandra://user:password@host:9042/keyspace`
- `cassandra://host:9042/keyspace?page_size=1000&consistency=ONE`

TLS is configured with the `enable_tls`, `tls_ca_path`, `tls_cert_path`,
`tls_key_path`, `tls_skip_verify`, and `tls_hostname_override` query
parameters. See [go/docs/cassandra.md](go/docs/cassandra.md) for all connection
options, supported features, and type mappings.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
