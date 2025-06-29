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

import datetime
from google.cloud.spanner_admin_database_v1 import UpdateDatabaseDdlRequest
from google.cloud.spanner_dbapi.parsed_statement import AutocommitDmlMode
from sqlalchemy import (
    create_engine,
    select,
    MetaData,
    Table,
    Column,
    Index,
    Integer,
    String,
    func,
    text,
    BigInteger,
    Enum,
)
from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column
from sqlalchemy.testing import eq_, is_instance_of
from google.cloud.spanner_v1 import (
    FixedSizePool,
    BatchCreateSessionsRequest,
    ExecuteSqlRequest,
    ResultSet,
    PingingPool,
    TypeCode,
)
from test.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_select1_result,
    add_result,
    add_single_result,
    add_update_count,
    add_singer_query_result,
)


class TestBasics(MockServerTestBase):
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
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": PingingPool(size=10)},
        )
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            results = connection.execute(
                select(1).execution_options(request_tag="my-tag")
            ).fetchall()
            self.verify_select1(results)
            request: ExecuteSqlRequest = self.spanner_service.requests[1]
            eq_("my-tag", request.request_options.request_tag)

    def test_sqlalchemy_select_now(self):
        now = datetime.datetime.now(datetime.UTC)
        iso_now = now.isoformat().replace("+00:00", "Z")
        add_single_result(
            "SELECT current_timestamp AS now_1",
            "now_1",
            TypeCode.TIMESTAMP,
            [(iso_now,)],
        )
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": PingingPool(size=10)},
        )
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            spanner_now = connection.execute(select(func.now())).fetchone()[0]
            eq_(spanner_now.timestamp(), now.timestamp())

    def test_create_table(self):
        add_result(
            """SELECT true
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA="" AND TABLE_NAME="users"
LIMIT 1
""",
            ResultSet(),
        )
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": FixedSizePool(size=10)},
        )
        metadata = MetaData()
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(16), nullable=False),
            Column("status", Enum("a", "b", "cee", create_constraint=True)),
        )
        metadata.create_all(engine)
        requests = self.database_admin_service.requests
        eq_(1, len(requests))
        is_instance_of(requests[0], UpdateDatabaseDdlRequest)
        eq_(1, len(requests[0].statements))
        eq_(
            "CREATE TABLE users (\n"
            "\tuser_id INT64 NOT NULL "
            "GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE), \n"
            "\tuser_name STRING(16) NOT NULL, \n"
            "\tstatus STRING(3), \n"
            "\tCHECK (status IN ('a', 'b', 'cee'))\n"
            ") PRIMARY KEY (user_id)",
            requests[0].statements[0],
        )

    def test_create_table_in_schema(self):
        add_result(
            """SELECT true
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA="schema" AND TABLE_NAME="users"
LIMIT 1
""",
            ResultSet(),
        )
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": FixedSizePool(size=10)},
        )
        metadata = MetaData()
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(16), nullable=False),
            Index("ix_users_user_id", "user_id"),
            schema="schema",
        )
        metadata.create_all(engine)
        requests = self.database_admin_service.requests
        eq_(1, len(requests))
        is_instance_of(requests[0], UpdateDatabaseDdlRequest)
        eq_(2, len(requests[0].statements))

        eq_(
            "CREATE TABLE schema.users (\n"
            "\tuser_id INT64 NOT NULL "
            "GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE), \n"
            "\tuser_name STRING(16) NOT NULL\n"
            ") PRIMARY KEY (user_id)",
            requests[0].statements[0],
        )
        eq_(
            "CREATE INDEX schema.ix_users_user_id ON schema.users (user_id)",
            requests[0].statements[1],
        )

    def test_create_multiple_tables(self):
        for i in range(2):
            add_result(
                f"""SELECT true
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA="" AND TABLE_NAME="table{i}"
LIMIT 1
""",
                ResultSet(),
            )
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": FixedSizePool(size=10)},
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
            eq_(
                f"CREATE TABLE table{i} (\n"
                "\tid INT64 NOT NULL "
                "GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE), \n"
                "\tvalue STRING(16) NOT NULL"
                "\n) PRIMARY KEY (id)",
                requests[0].statements[i],
            )

    def test_partitioned_dml(self):
        sql = "UPDATE singers SET checked=true WHERE active = true"
        add_update_count(sql, 100, AutocommitDmlMode.PARTITIONED_NON_ATOMIC)
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={
                "client": self.client,
                "pool": PingingPool(size=10),
                "ignore_transaction_warnings": True,
            },
        )
        # TODO: Support autocommit_dml_mode as a connection variable in execution
        #       options.
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            connection.connection.set_autocommit_dml_mode(
                AutocommitDmlMode.PARTITIONED_NON_ATOMIC
            )
            results = connection.execute(text(sql)).rowcount
            eq_(100, results)

    def test_select_for_update(self):
        class Base(DeclarativeBase):
            pass

        class Singer(Base):
            __tablename__ = "singers"
            id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
            name: Mapped[str] = mapped_column(String)

        query = (
            "SELECT singers.id AS singers_id, singers.name AS singers_name\n"
            "FROM singers\n"
            "WHERE singers.id = @a0\n"
            " LIMIT @a1 FOR UPDATE"
        )
        add_singer_query_result(query)
        update = "UPDATE singers SET name=@a0 WHERE singers.id = @a1"
        add_update_count(update, 1)

        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": FixedSizePool(size=10)},
        )

        with Session(engine) as session:
            singer = (
                session.query(Singer).filter(Singer.id == 1).with_for_update().first()
            )
            singer.name = "New Name"
            session.add(singer)
            session.commit()

    def test_database_role(self):
        add_select1_result()
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={
                "client": self.client,
                "pool": FixedSizePool(size=10),
                "database_role": "my_role",
            },
        )
        with Session(engine.execution_options(isolation_level="autocommit")) as session:
            session.execute(select(1))
        requests = self.spanner_service.requests
        eq_(2, len(requests))
        is_instance_of(requests[0], BatchCreateSessionsRequest)
        is_instance_of(requests[1], ExecuteSqlRequest)
        request: BatchCreateSessionsRequest = requests[0]
        eq_("my_role", request.session_template.creator_role)

    def test_select_table_in_named_schema(self):
        class Base(DeclarativeBase):
            pass

        class Singer(Base):
            __tablename__ = "singers"
            __table_args__ = {"schema": "my_schema"}
            id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
            name: Mapped[str] = mapped_column(String)

        query = (
            "SELECT"
            " singers_1.id AS my_schema_singers_id,"
            " singers_1.name AS my_schema_singers_name\n"
            "FROM my_schema.singers AS singers_1\n"
            "WHERE singers_1.id = @a0\n"
            " LIMIT @a1"
        )
        add_singer_query_result(query)
        engine = create_engine(
            "spanner:///projects/p/instances/i/databases/d",
            connect_args={"client": self.client, "pool": FixedSizePool(size=10)},
        )

        insert = "INSERT INTO my_schema.singers (name) VALUES (@a0) THEN RETURN id"
        add_single_result(insert, "id", TypeCode.INT64, [("1",)])
        with Session(engine) as session:
            singer = Singer(name="New Name")
            session.add(singer)
            session.commit()

        update = (
            "UPDATE my_schema.singers AS singers_1 "
            "SET name=@a0 "
            "WHERE singers_1.id = @a1"
        )
        add_update_count(update, 1)
        with Session(engine) as session:
            singer = session.query(Singer).filter(Singer.id == 1).first()
            singer.name = "New Name"
            session.add(singer)
            session.commit()

        delete = "DELETE FROM my_schema.singers AS singers_1 WHERE singers_1.id = @a0"
        add_update_count(delete, 1)
        with Session(engine) as session:
            singer = session.query(Singer).filter(Singer.id == 1).first()
            session.delete(singer)
            session.commit()
