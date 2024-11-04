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
from sqlalchemy import Engine, create_engine
from sqlalchemy.testing.plugin.plugin_base import fixtures
import google.cloud.spanner_v1.types.type as spanner_type
import google.cloud.spanner_v1.types.result_set as result_set
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud.spanner_v1 import (
    Client,
    ResultSet,
    PingingPool,
)
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
import grpc

# TODO: Replace this with the mock server in the Spanner client lib
from test.mockserver_tests.mock_spanner import SpannerServicer, start_mock_server
from test.mockserver_tests.mock_database_admin import DatabaseAdminServicer


def add_result(sql: str, result: ResultSet):
    MockServerTestBase.spanner_service.mock_spanner.add_result(sql, result)


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
    MockServerTestBase.spanner_service.mock_spanner.add_result("select 1", result)


class MockServerTestBase(fixtures.TestBase):
    server: grpc.Server = None
    spanner_service: SpannerServicer = None
    database_admin_service: DatabaseAdminServicer = None
    port: int = None

    @classmethod
    def setup_class(cls):
        (
            MockServerTestBase.server,
            MockServerTestBase.spanner_service,
            MockServerTestBase.database_admin_service,
            MockServerTestBase.port,
        ) = start_mock_server()

    @classmethod
    def teardown_class(cls):
        if MockServerTestBase.server is not None:
            MockServerTestBase.server.stop(grace=None)
            MockServerTestBase.server = None

    def setup_method(self):
        self._client = None
        self._instance = None
        self._database = None

    def teardown_method(self):
        MockServerTestBase.spanner_service.clear_requests()
        MockServerTestBase.database_admin_service.clear_requests()

    def create_engine(self) -> Engine:
        return create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": PingingPool(size=10)},
        )

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = Client(
                project="p",
                credentials=AnonymousCredentials(),
                client_options=ClientOptions(
                    api_endpoint="localhost:" + str(MockServerTestBase.port),
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
                "test-database", pool=PingingPool(size=10)
            )
        return self._database
