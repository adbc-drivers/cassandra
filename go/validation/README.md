<!--
  Copyright (c) 2025 ADBC Drivers Contributors

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

1. Start Cassandra and initialize the test keyspaces:

   ```shell
   docker compose up --detach --wait
   docker compose exec -T test-service cqlsh -f /docker-entrypoint-initdb.d/init.cql
   ```

2. Load the local test configuration:

   ```shell
   source .env.linux
   ```

3. Build the shared driver and run validation:

   ```shell
   pixi run make
   pixi run validate
   ```

Run an individual validation module with:

```shell
pixi run pytest -v validation/tests/test_ingest.py
```

Stop Cassandra with `docker compose down`. Add `--volumes` to remove its data.
