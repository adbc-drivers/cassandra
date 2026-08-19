# Copyright (c) 2026 ADBC Drivers Contributors
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
import pytest


def test_package() -> None:
    uri = "cassandra://127.0.0.1:1/adbc_test?connect_timeout=100&timeout=100"

    with pytest.raises(
        adbc_driver_manager.dbapi.OperationalError,
        match="failed to create session",
    ):
        with adbc_driver_manager.dbapi.connect(driver="cassandra", uri=uri):
            pass
