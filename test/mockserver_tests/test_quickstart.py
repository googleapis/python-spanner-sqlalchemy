# Copyright 2024 Google LLC All rights reserved.
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

from google.cloud.spanner_admin_database_v1 import UpdateDatabaseDdlRequest
from google.cloud.spanner_v1 import (
    ResultSet,
    ResultSetStats,
    BatchCreateSessionsRequest,
    ExecuteBatchDmlRequest,
    CommitRequest,
    BeginTransactionRequest,
)
from sqlalchemy.orm import Session
from sqlalchemy.testing import eq_, is_instance_of, is_not_none
from test.mockserver_tests.mock_server_test_base import MockServerTestBase, add_result


class TestQuickStart(MockServerTestBase):
    def test_create_tables(self):
        from test.mockserver_tests.quickstart_model import Base

        # TODO: Fix the double quotes inside these SQL fragments.
        add_result(
            """SELECT true
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA="" AND TABLE_NAME="user_account"
LIMIT 1""",
            ResultSet(),
        )
        add_result(
            """SELECT true
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA="" AND TABLE_NAME="address"
LIMIT 1""",
            ResultSet(),
        )

        engine = self.create_engine()
        Base.metadata.create_all(engine)
        requests = self.database_admin_service.requests
        eq_(1, len(requests))
        is_instance_of(requests[0], UpdateDatabaseDdlRequest)
        eq_(2, len(requests[0].statements))
        eq_(
            "CREATE TABLE user_account (\n"
            "\tid INT64 NOT NULL, \n"
            "\tname STRING(30) NOT NULL, \n"
            "\tfullname STRING(MAX)\n"
            ") PRIMARY KEY (id)",
            requests[0].statements[0],
        )
        eq_(
            "CREATE TABLE address (\n"
            "\tid INT64 NOT NULL, \n"
            "\temail_address STRING(MAX) NOT NULL, \n"
            "\tuser_id INT64 NOT NULL, \n"
            "\tFOREIGN KEY(user_id) REFERENCES user_account (id)\n"
            ") PRIMARY KEY (id)",
            requests[0].statements[1],
        )

    def test_insert_data(self):
        from test.mockserver_tests.quickstart_model import User, Address

        # TODO: Use auto-generated primary keys.
        update_count = ResultSet(
            dict(
                stats=ResultSetStats(
                    dict(
                        row_count_exact=1,
                    )
                )
            )
        )
        add_result(
            "INSERT INTO user_account (id, name, fullname) VALUES (@a0, @a1, @a2)",
            update_count,
        )
        add_result(
            "INSERT INTO address (id, email_address, user_id) VALUES (@a0, @a1, @a2)",
            update_count,
        )

        engine = self.create_engine()
        with Session(engine) as session:
            spongebob = User(
                id=1,
                name="spongebob",
                fullname="Spongebob Squarepants",
                addresses=[Address(id=1, email_address="spongebob@sqlalchemy.org")],
            )
            sandy = User(
                id=2,
                name="sandy",
                fullname="Sandy Cheeks",
                addresses=[
                    Address(id=2, email_address="sandy@sqlalchemy.org"),
                    Address(id=3, email_address="sandy@squirrelpower.org"),
                ],
            )
            patrick = User(id=3, name="patrick", fullname="Patrick Star")
            session.add_all([spongebob, sandy, patrick])
            session.commit()

            requests = self.spanner_service.requests
            eq_(5, len(requests))
            is_instance_of(requests[0], BatchCreateSessionsRequest)
            is_instance_of(requests[1], BeginTransactionRequest)
            is_instance_of(requests[2], ExecuteBatchDmlRequest)
            is_instance_of(requests[3], ExecuteBatchDmlRequest)
            is_instance_of(requests[4], CommitRequest)
            is_not_none(requests[2].transaction.id)
            eq_(requests[2].transaction.id, requests[3].transaction.id)
            eq_(requests[2].transaction.id, requests[4].transaction_id)
