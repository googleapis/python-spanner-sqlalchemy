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
from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer, \
    String
from sqlalchemy.testing import eq_, is_instance_of
from sqlalchemy.testing.plugin.plugin_base import fixtures
import google.cloud.spanner_v1.types.type as spanner_type
import google.cloud.spanner_v1.types.result_set as result_set
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud.spanner_v1 import (
    Client,
    FixedSizePool,
    BatchCreateSessionsRequest,
    ExecuteSqlRequest,
)
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
import grpc

# TODO: Replace this with the mock server in the Spanner client lib
from test.mockserver_tests.mock_spanner import SpannerServicer, \
    start_mock_server
from test.mockserver_tests.mock_database_admin import DatabaseAdminServicer

def add_select1_result():
    result = result_set.ResultSet(
        dict(
            metadata=result_set.ResultSetMetadata(
                dict(
                    row_type=spanner_type.StructType(
                        dict(
                            fields=[
                                spanner_type.StructType.Field(
                                    dict(
                                        name="c",
                                        type=spanner_type.Type(
                                            dict(code=spanner_type.TypeCode.INT64)
                                        ),
                                    )
                                )
                            ]
                        )
                    )
                )
            ),
        )
    )
    result.rows.extend(["1"])
    TestBasics.spanner_service.mock_spanner.add_result("select 1", result)


class TestBasics(fixtures.TestBase):
    server: grpc.Server = None
    spanner_service: SpannerServicer = None
    database_admin_service: DatabaseAdminServicer = None
    port: int = None

    @classmethod
    def setup_class(cls):
        (
            TestBasics.server,
            TestBasics.spanner_service,
            TestBasics.database_admin_service,
            TestBasics.port,
        ) = start_mock_server()

    @classmethod
    def teardown_class(cls):
        if TestBasics.server is not None:
            TestBasics.server.stop(grace=None)
            TestBasics.server = None

    def setup_method(self):
        self._client = None
        self._instance = None
        self._database = None

    def teardown_method(self):
        TestBasics.spanner_service.clear_requests()
        TestBasics.database_admin_service.clear_requests()

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = Client(
                project="test-project",
                credentials=AnonymousCredentials(),
                client_options=ClientOptions(
                    api_endpoint="localhost:" + str(TestBasics.port),
                ),
            )
        return self._client

    @property
    def instance(self) -> Instance:
        if self._instance is None:
            self._instance = self.client.instance("test-instance")
        return self._instance

    @property
    def database(self) -> Database:
        if self._database is None:
            self._database = self.instance.database(
                "test-database", pool=FixedSizePool(size=10)
            )
        return self._database

    def verify_select1(self, results):
        result_list = []
        for row in results:
            result_list.append(row)
            eq_(1, row[0])
        eq_(1, len(result_list))
        requests = self.spanner_service.requests
        eq_(2, len(requests))
        is_instance_of(requests[0], BatchCreateSessionsRequest)
        is_instance_of(requests[1], ExecuteSqlRequest)

    def test_select1(self):
        add_select1_result()
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("select 1")
            self.verify_select1(results)

    def test_sqlalchemy_select1(self):
        add_select1_result()
        engine = create_engine(
            "spanner:///projects/test-project/instances/test-instance/databases/test-database",
            connect_args={'client': self.client, 'pool': FixedSizePool(size=10)},
        )
        with engine.begin() as connection:
            results = connection.execute(select(1)).fetchall()
            self.verify_select1(results)

    def test_create_table(self):
        engine = create_engine(
            "spanner:///projects/test-project/instances/test-instance/databases/test-database",
            connect_args={'client': self.client, 'pool': FixedSizePool(size=10)},
        )
        metadata = MetaData()
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(16), nullable=False),
        )
        metadata.create_all(engine)
        requests = self.database_admin_service.requests
        eq_(1, len(requests))
        is_instance_of(requests[0], UpdateDatabaseDdlRequest)
        eq_(1, len(requests[0].statements))
        eq_("CREATE TABLE users (\n"
            "\tuser_id INT64 NOT NULL, \n"
            "\tuser_name STRING(16) NOT NULL\n"
            ") PRIMARY KEY (user_id)", requests[0].statements[0])

    def test_create_multiple_tables(self):
        engine = create_engine(
            "spanner:///projects/test-project/instances/test-instance/databases/test-database",
            connect_args={'client': self.client, 'pool': FixedSizePool(size=10)},
        )
        metadata = MetaData()
        for i in range(2):
            Table(
                "table" + str(i),
                metadata,
                Column("id", Integer, primary_key=True),
                Column("value", String(16), nullable=False),
            )
        metadata.create_all(engine)
        requests = self.database_admin_service.requests
        eq_(1, len(requests))
        is_instance_of(requests[0], UpdateDatabaseDdlRequest)
        eq_(2, len(requests[0].statements))
        for i in range(2):
            eq_(f"CREATE TABLE table{i} (\n"
                "\tid INT64 NOT NULL, \n"
                "\tvalue STRING(16) NOT NULL"
                "\n) PRIMARY KEY (id)", requests[0].statements[i])
