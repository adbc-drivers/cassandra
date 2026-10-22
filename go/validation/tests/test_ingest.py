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
from adbc_drivers_validation import compare, model, utils
from adbc_drivers_validation.tests.ingest import make_table_name
from adbc_drivers_validation.utils import execute_query_without_prepare

from . import cassandra


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    def test_create_dictionary(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: model.Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_create_dictionary", query)
        plain = subquery.input()
        data = pyarrow.concat_tables([plain, plain]).combine_chunks()
        values = data.column(1).dictionary_encode()
        data = data.set_column(1, data.schema.field(1).with_type(values.type), values)

        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table_name)
            with driver.setup_statement(query, cursor):
                modified = cursor.adbc_ingest(table_name, data, mode="create")
                if driver.features.statement_rows_affected:
                    assert modified == len(data)
                else:
                    assert modified == -1

        fields = [driver.quote_identifier(field.name) for field in data.schema]
        select = (
            f"SELECT {', '.join(fields)} FROM {driver.quote_identifier(table_name)}"
        )
        with conn.cursor() as cursor, driver.setup_statement(query, cursor):
            result = execute_query_without_prepare(cursor, select)

        # Cassandra, DSE, and ScyllaDB upsert duplicate primary keys, so repeating
        # the fixture exercises repeated dictionary indices without adding rows.
        expected = subquery.expected()
        expected = expected.sort_by(expected.schema.names[0])
        result = result.sort_by(result.schema.names[0])
        compare.compare_tables(expected, result, query.metadata())
        subschema = pyarrow.schema(list(result.schema)[1:])
        utils.assert_field_type_name(driver, "query", query, subschema)

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
