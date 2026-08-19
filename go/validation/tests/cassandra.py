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

import functools
import os
import re
from pathlib import Path

from adbc_drivers_validation import model
from adbc_drivers_validation.quirks import split_statement


class CassandraQuirks(model.DriverQuirks):
    name = "cassandra"
    driver = "adbc_driver_cassandra"
    driver_name = "ADBC Driver Foundry Driver for Cassandra"
    vendor_name = "Cassandra"
    vendor_version = re.compile(r"5\.0\.\d+")
    short_version = "5.0"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        # Cassandra doesn't have catalogs
        connection_set_current_catalog=False,
        # The driver reports the current keyspace but cannot switch it after connect.
        connection_set_current_schema=False,
        # Cassandra doesn't support transactions
        connection_transactions=False,
        # Cassandra doesn't have traditional foreign/primary key constraints
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_prepare=True,
        # Cassandra doesn't return rows affected
        statement_rows_affected=False,
        current_catalog=None,
        current_schema=model.FromEnv("CASSANDRA_KEYSPACE"),
        secondary_schema=os.getenv("CASSANDRA_SECONDARY_KEYSPACE"),
    )
    setup = model.DriverSetup(
        database={
            "cassandra.hosts": model.FromEnv("CASSANDRA_HOSTS"),
            "cassandra.port": model.FromEnv("CASSANDRA_PORT"),
            "cassandra.keyspace": model.FromEnv("CASSANDRA_KEYSPACE"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries/cassandra",)

    def query_override(self, context: str, default: str) -> str:
        """Override ad-hoc framework queries when Cassandra needs different CQL."""
        if context == "TestStatement.sample_table":
            quoted_name = self.quote_identifier("sample_table")
            return f"CREATE TABLE {quoted_name} (id INT PRIMARY KEY, value TEXT)"
        if context == "TestConnection.test_get_table_schema_schema":
            quoted_name = self.quote_identifier(
                self.features.secondary_schema, "test_get_table_schema_schema"
            )
            return f"CREATE TABLE {quoted_name} (spam INT PRIMARY KEY, eggs VARCHAR)"
        if context.startswith("TestStatement.test_rows_affected."):
            quoted_name = self.quote_identifier("test_rows_affected")
            id_ = self.quote_identifier("id")
            value = self.quote_identifier("value")
            operation = context.rsplit(".", 1)[-1]
            overrides = {
                "create_table": (
                    f"CREATE TABLE {quoted_name} ({id_} INT PRIMARY KEY, {value} INT)"
                ),
                "insert": (f"INSERT INTO {quoted_name} ({id_}, {value}) VALUES (1, 1)"),
                "update": f"UPDATE {quoted_name} SET {value} = 2 WHERE {id_} = 1",
                "delete": f"DELETE FROM {quoted_name} WHERE {id_} = 1",
            }
            return overrides[operation]
        if context == "TestStatement.test_nonascii_queries":
            literal = default.removeprefix("SELECT ").removesuffix(" AS greeting")
            return (
                f"SELECT blobAsText(textAsBlob({literal})) AS greeting "
                "FROM system.local"
            )
        return default

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        return (
            "unconfigured table" in str(error).lower()
            or "does not exist" in str(error).lower()
        )

    def quote_one_identifier(self, identifier: str) -> str:
        # Cassandra uses double quotes for identifiers
        return f'"{identifier}"'

    def split_statement(self, statement: str) -> list[str]:
        """Split statements for Cassandra (no dialect needed)."""
        # Cassandra uses standard CQL, no special dialect needed
        return split_statement(statement, dialect=None)


class DSEQuirks(CassandraQuirks):
    name = "dse"
    # DSE 6.9.25 reports its Cassandra-compatible release version through
    # system.local rather than returning the DSE image tag.
    vendor_version = "4.0.0.6925"
    short_version = "6.9"
    features = CassandraQuirks.features.with_values(
        current_schema=model.FromEnv("DSE_KEYSPACE"),
        secondary_schema=model.FromEnv("DSE_SECONDARY_KEYSPACE"),
    )
    setup = model.DriverSetup(
        database={
            "cassandra.hosts": model.FromEnv("DSE_HOSTS"),
            "cassandra.port": model.FromEnv("DSE_PORT"),
            "cassandra.keyspace": model.FromEnv("DSE_KEYSPACE"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return super().queries_paths + (Path(__file__).parent.parent / "queries/dse",)


@functools.cache
def get_quirks(test_config: str) -> CassandraQuirks:
    """Get quirks for a validation target."""
    if test_config == "cassandra":
        return CassandraQuirks()
    if test_config == "dse":
        return DSEQuirks()
    raise ValueError(f"unsupported test config: {test_config}")
