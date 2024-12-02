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
from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import Session
from sqlalchemy.testing import (
    eq_,
    is_instance_of,
    is_not_none,
    is_none,
    is_false,
    is_true,
)
from google.cloud.spanner_v1 import (
    FixedSizePool,
    BatchCreateSessionsRequest,
    ExecuteSqlRequest,
    ResultSet,
    PingingPool,
    ResultSetStats,
    BeginTransactionRequest,
    ExecuteBatchDmlRequest,
    CommitRequest,
    TypeCode,
)
from test.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_select1_result,
    add_result,
)


class TestFloat32(MockServerTestBase):
    def test_insert_data(self):
        from test.mockserver_tests.float32_model import Number

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
            "INSERT INTO numbers (number, name, ln) VALUES (@a0, @a1, @a2)",
            update_count,
        )

        engine = self.create_engine()
        with Session(engine) as session:
            n1 = Number(number=1, name="One", ln=0.0)
            session.add_all([n1])
            session.commit()

            requests = self.spanner_service.requests
            eq_(4, len(requests))
            is_instance_of(requests[0], BatchCreateSessionsRequest)
            is_instance_of(requests[1], BeginTransactionRequest)
            is_instance_of(requests[2], ExecuteSqlRequest)
            is_instance_of(requests[3], CommitRequest)
            request: ExecuteSqlRequest = requests[2]
            eq_(3, len(request.params))
            eq_("1", request.params["a0"])
            eq_("One", request.params["a1"])
            eq_(0.0, request.params["a2"])
            eq_(TypeCode.INT64, request.param_types["a0"].code)
            eq_(TypeCode.STRING, request.param_types["a1"].code)
            is_false("a2" in request.param_types)
