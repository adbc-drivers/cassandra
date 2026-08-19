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

An [ADBC](https://arrow.apache.org/adbc/) driver for
[Apache Cassandra](https://cassandra.apache.org/), implemented in Go.

## Installation

```sh
go get github.com/adbc-drivers/cassandra/go
```

## Documentation

Driver configuration, supported features, and type mappings are documented in
[go/docs/cassandra.md](go/docs/cassandra.md).

## Building

The Go module, Pixi environment, validation suite, and Docker Compose setup are
under [`go/`](go/). See [CONTRIBUTING.md](CONTRIBUTING.md) for build and test
instructions.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for issue reporting, development setup,
and pull request guidelines.
