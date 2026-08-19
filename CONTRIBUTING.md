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

# How to Contribute

All contributors are expected to follow the [Code of
Conduct](https://github.com/adbc-drivers/cassandra?tab=coc-ov-file#readme).

## Reporting Bugs and Requesting Features

Search the [GitHub issue
tracker](https://github.com/adbc-drivers/cassandra/issues) for an existing issue
before filing a bug report or feature request. Include enough detail to
reproduce bugs and explain the intended use case for new features.

Potential security vulnerabilities must be reported to
[security@adbc-drivers.org](mailto:security@adbc-drivers.org), not through a
public issue. See the repository [Security
Policy](https://github.com/adbc-drivers/cassandra?tab=security-ov-file#readme).

## Developer Environment

Install these prerequisites:

- [Go](https://go.dev/) at the version declared in `go/go.mod`
- [Pixi](https://pixi.sh/)
- Docker with Compose support
- [pre-commit](https://pre-commit.com/)

From the `go/` directory, start and initialize Cassandra:

```shell
docker compose up --detach --wait
docker compose exec -T test-service \
  cqlsh -f /docker-entrypoint-initdb.d/init.cql
source .env.linux
```

Build and test the Go module and shared driver:

```shell
go build ./...
go test -tags assert -v ./...
pixi run make
pixi run validate
```

See the [validation setup](go/validation/README.md) for more details. Stop the
local Cassandra instance with `docker compose down` when finished.

The Docker Compose environment runs a single Cassandra node, exposes CQL
on port 9042, and initializes the `adbc_test` and `adbc_test2` keyspaces with
replication factor 1. Useful commands include:

```shell
docker compose logs test-service
docker compose exec test-service cqlsh
docker compose exec test-service nodetool status
```

Without a reachable Cassandra host, the Go test command skips the
Cassandra-backed integration suites and runs only unit coverage.

To render the validation report as documentation after running validation:

```shell
pixi run gendocs --output build/docs
```

## Development

Before opening a pull request:

- Review the diff and remove generated or unrelated files.
- Add the Apache license header to new files.
- Run `pre-commit run --all-files` from the repository root after staging or
  committing changes. The Apache RAT hook does not inspect untracked files.
- Run the relevant Go and validation tests.

Pull request titles must follow [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/). Use the `go`
component for Go driver changes or omit the component for repository-wide
maintenance. For example:

- `feat(go): support a new Cassandra type`
- `fix(go): propagate query cancellation`
- `chore: update action versions`

Mark breaking changes with `!`. End the pull request description with `Closes
#NNN`, `Fixes #NNN`, or similar so GitHub links the associated issue.
