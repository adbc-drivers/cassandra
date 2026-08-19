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

import adbc_driver_manager
import adbc_drivers_validation.tests.statement
import pyarrow

from . import cassandra


def pytest_generate_tests(metafunc) -> None:
    quirks = [cassandra.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.statement.generate_tests(quirks, metafunc)


class TestStatement(adbc_drivers_validation.tests.statement.TestStatement):
    def test_parameter_execute(self, driver, conn) -> None:
        table_name = "test_parameter_execute"
        quoted_name = driver.quote_identifier(table_name)

        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name=table_name)
            )
            try:
                cursor.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            cursor.adbc_statement.set_sql_query(
                f"CREATE TABLE {quoted_name} (id INT PRIMARY KEY, value INT)"
            )
            cursor.adbc_statement.execute_update()

            cursor.adbc_statement.set_sql_query(
                f"INSERT INTO {quoted_name} (id, value) VALUES (?, ?)"
            )
            insert_parameters = pyarrow.RecordBatch.from_pydict(
                {"0": [1, 2, 3, 4], "1": [10, 20, 30, 40]}
            )
            cursor.adbc_statement.bind(insert_parameters)
            cursor.adbc_statement.prepare()
            cursor.adbc_statement.execute_update()

            cursor.adbc_statement.set_sql_query(
                f"SELECT value FROM {quoted_name} WHERE id = ?"
            )
            query_parameters = pyarrow.RecordBatch.from_pydict({"0": [1, 2, 3, 4]})
            cursor.adbc_statement.bind(query_parameters)
            cursor.adbc_statement.prepare()
            handle, _ = cursor.adbc_statement.execute_query()
            result = pyarrow.RecordBatchReader._import_from_c(handle.address).read_all()

            assert result[0].to_pylist() == [10, 20, 30, 40]

            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name=table_name)
            )
            try:
                cursor.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
