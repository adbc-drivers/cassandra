# Copyright (c) 2025 ADBC Drivers Contributors
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

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.ingest
import pyarrow
from adbc_drivers_validation import model

from . import cassandra


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    def test_many_columns(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        num_cols = 100
        num_rows = 1000
        table_name = "test_ingest_many_columns"

        data = pyarrow.table(
            {f"col_{i}": list(range(num_rows)) for i in range(num_cols)}
        ).replace_schema_metadata({"cassandra:primary_key": "col_0"})

        with conn.cursor() as cursor:
            with driver.setup_statement("TestIngest.test_many_columns", cursor):
                modified = cursor.adbc_ingest(table_name, data, mode="replace")
            if driver.features.statement_rows_affected:
                assert modified == num_rows
            else:
                assert modified == -1

            count = driver.query_override(
                "TestIngest.test_many_columns",
                f"SELECT COUNT(*) FROM {driver.quote_identifier(table_name)}",
            )
            cursor.execute(count)
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == num_rows


def pytest_generate_tests(metafunc) -> None:
    quirks = [cassandra.get_quirks(metafunc.config.getoption("vendor_version"))]
    adbc_drivers_validation.tests.ingest.generate_tests(quirks, metafunc)
