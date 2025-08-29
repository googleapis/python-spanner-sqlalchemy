# Copyright 2025 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sqlalchemy import create_engine
from sqlalchemy.testing import eq_, is_instance_of
from google.cloud.spanner_v1 import (
    FixedSizePool,
    ResultSet,
)
from test.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_result,
)
from google.cloud.spanner_admin_database_v1 import UpdateDatabaseDdlRequest


class TestNullFilteredIndex(MockServerTestBase):
    """Ensure we emit correct DDL for not null filtered indexes."""

    def test_create_table(self):
        from test.mockserver_tests.null_filtered_index import Base

        add_result(
            """SELECT true
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA="" AND TABLE_NAME="singers"
LIMIT 1
""",
            ResultSet(),
        )
        add_result(
            """SELECT true
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA="" AND TABLE_NAME="albums"
LIMIT 1
""",
            ResultSet(),
        )
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": FixedSizePool(size=10)},
        )
        Base.metadata.create_all(engine)
        requests = self.database_admin_service.requests
        eq_(1, len(requests))
        is_instance_of(requests[0], UpdateDatabaseDdlRequest)
        eq_(5, len(requests[0].statements))
        eq_(
            "CREATE TABLE singers (\n"
            "\tid STRING(MAX) NOT NULL, \n"
            "\tname STRING(MAX) NOT NULL\n"
            ") PRIMARY KEY (id)",
            requests[0].statements[0],
        )

        # The order of the CREATE INDEX statements appears to be
        # arbitrary, so we sort it for test consistency.
        index_statements = sorted(requests[0].statements[1:])
        eq_("CREATE INDEX idx_name ON singers (name)", index_statements[0])
        eq_(
            "CREATE NULL_FILTERED INDEX idx_null_filtered_name ON singers (name)",
            index_statements[1],
        )
        eq_("CREATE UNIQUE INDEX idx_uq_name ON singers (name)", index_statements[2])
        eq_(
            "CREATE UNIQUE NULL_FILTERED INDEX "
            "idx_uq_null_filtered_name ON singers (name)",
            index_statements[3],
        )
