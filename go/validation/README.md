<!--
  Copyright (c) 2025-2026 ADBC Drivers Contributors

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

# Validation Suite Setup

Run these commands from the `go` directory.

1. Start Cassandra and DataStax Enterprise, then initialize their test
   keyspaces:

   ```shell
   docker compose up --detach --wait test-service dse
   docker compose exec -T test-service cqlsh -f /docker-entrypoint-initdb.d/init.cql
   docker compose exec -T dse cqlsh -f /docker-entrypoint-initdb.d/init.cql
   ```

   Starting the DSE service accepts the DataStax license through the
   `DS_LICENSE=accept` setting in `compose.yaml`.

2. Load the local test configuration:

   ```shell
   source .env.linux
   ```

3. Build the shared driver and run validation:

   ```shell
   pixi run make
   pixi run validate --vendor-version cassandra
   pixi run validate --vendor-version dse
   ```

Run an individual validation module with:

```shell
pixi run pytest -v validation/tests/test_ingest.py
```

Stop the services with `docker compose down`. Add `--volumes` to remove their
data.
