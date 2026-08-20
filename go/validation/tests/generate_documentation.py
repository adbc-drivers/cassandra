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

import argparse
import functools
from pathlib import Path

from adbc_drivers_validation import generate_documentation, model

from . import cassandra


@functools.cache
def get_quirks(version: str, *, vendor: str) -> model.DriverQuirks:
    if vendor == "cassandra" and version == "5.0":
        return cassandra.CassandraQuirks()
    if vendor == "dse" and version == "6.9":
        return cassandra.DSEQuirks()
    if vendor not in ("cassandra", "dse"):
        raise ValueError(f"unsupported vendor: {vendor}")
    raise ValueError(f"unsupported {vendor} version: {version}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    template = Path(__file__).parent.parent.parent / "docs/cassandra.md"
    reports = [report.resolve() for report in Path(".").glob("validation-report*.xml")]

    generate_documentation.generate(
        "cassandra",
        get_quirks,
        [
            ("cassandra", "Apache Cassandra"),
            ("dse", "DataStax Enterprise"),
        ],
        reports,
        template.resolve(),
        args.output.resolve(),
    )
