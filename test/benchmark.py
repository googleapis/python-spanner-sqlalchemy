# Copyright 2021 Google LLC
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
import pprint
import time

from google.cloud import spanner_dbapi
from google.cloud.spanner_v1 import Client, KeySet
from sqlalchemy import (
    create_engine,
    insert,
    select,
    text,
    MetaData,
    Table,
)


def measure_execution_time(function):
    def wrapper(self, measures):
        t_start = time.time()
        function(self)
        measures[function.__name__] = round(time.time() - t_start, 2)

    return wrapper


class BenchmarkTestBase:
    def __init__(self):
        self._create_table()

        self._one_row = (
            1,
            "Pete",
            "Allison",
            datetime.datetime(1998, 10, 6).strftime("%Y-%m-%d"),
            b"123",
        )

    def _cleanup(self):
        conn = spanner_dbapi.connect("sqlalchemy-dialect-test", "compliance-test")
        conn.database.update_ddl(["DROP TABLE Singers"])
        conn.close()

    def _create_table(self):
        conn = spanner_dbapi.connect("sqlalchemy-dialect-test", "compliance-test")
        conn.database.update_ddl(
            [
                """
CREATE TABLE Singers (
    id INT64,
    first_name STRING(1024),
    last_name STRING(1024),
    birth_date DATE,
    picture BYTES(1024),
) PRIMARY KEY (id)
        """
            ]
        ).result(120)

        conn.close()

    def run(self):
        measures = {}
        for method in (
            self.insert_one_row_with_fetch_after,
            self.read_one_row,
            self.insert_many_rows,
            self.select_many_rows,
        ):
            method(measures)

        self._cleanup()
        return measures


class SpannerBenchmarkTest(BenchmarkTestBase):
    def __init__(self):
        super().__init__()
        self._client = Client()
        self._instance = self._client.instance("sqlalchemy-dialect-test")
        self._database = self._instance.database("compliance-test")

        self._many_rows = []
        num = 1
        birth_date = datetime.datetime(1998, 10, 6).strftime("%Y-%m-%d")
        for i in range(99):
            num += 1
            self._many_rows.append((num, "Pete", "Allison", birth_date, b"123"))

    @measure_execution_time
    def insert_one_row_with_fetch_after(self):
        self._database.run_in_transaction(insert_one_row, self._one_row)

    @measure_execution_time
    def insert_many_rows(self):
        self._database.run_in_transaction(insert_many_rows, self._many_rows)

    @measure_execution_time
    def read_one_row(self):
        with self._database.snapshot() as snapshot:
            keyset = KeySet(keys=([1],))
            row = snapshot.read(
                table="Singers",
                columns=("id", "first_name", "last_name", "birth_date", "picture"),
                keyset=keyset,
            ).one()
            if not row:
                raise ValueError("No rows read")

    @measure_execution_time
    def select_many_rows(self):
        with self._database.snapshot() as snapshot:
            rows = list(
                snapshot.execute_sql("SELECT * FROM Singers ORDER BY last_name")
            )
            if len(rows) != 100:
                raise ValueError("Wrong number of rows read")


class SQLAlchemyBenchmarkTest(BenchmarkTestBase):
    def __init__(self):
        super().__init__()
        self._engine = create_engine(
            "spanner:///projects/appdev-soda-spanner-staging/instances/"
            "sqlalchemy-dialect-test/databases/compliance-test"
        )
        metadata = MetaData(bind=self._engine)
        self._table = Table("Singers", metadata, autoload=True)

        self._many_rows = []
        num = 1
        birth_date = datetime.datetime(1998, 10, 6).strftime("%Y-%m-%d")
        for i in range(99):
            num += 1
            self._many_rows.append(
                {
                    "id": num,
                    "first_name": "Pete",
                    "last_name": "Allison",
                    "birth_date": birth_date,
                    "picture": b"123",
                }
            )

    @measure_execution_time
    def insert_one_row_with_fetch_after(self):
        insert(self._table).values(self._one_row).execute()
        last_name = (
            select([text("last_name")], from_obj=self._table).execute().fetchone()
        )[0]
        if last_name != "Allison":
            raise ValueError("Received invalid last name: " + last_name)

    @measure_execution_time
    def insert_many_rows(self):
        with self._engine.begin() as conn:
            conn.execute(
                self._table.insert(), self._many_rows,
            )

    @measure_execution_time
    def read_one_row(self):
        row = select(["*"], from_obj=self._table).execute().fetchone()
        if not row:
            raise ValueError("No rows read")

    @measure_execution_time
    def select_many_rows(self):
        rows = (
            select(["*"], from_obj=self._table, order_by=("last_name",))
            .execute()
            .fetchall()
        )
        if len(rows) != 100:
            raise ValueError("Wrong number of rows read")


def insert_one_row(transaction, one_row):
    transaction.execute_update(
        "INSERT Singers (id, first_name, last_name, birth_date, picture) "
        " VALUES {}".format(str(one_row))
    )
    last_name = transaction.execute_sql(
        "SELECT last_name FROM Singers WHERE id=1"
    ).one()[0]
    if last_name != "Allison":
        raise ValueError("Received invalid last name: " + last_name)


def insert_many_rows(transaction, many_rows):
    total_count = 0
    for row in many_rows:
        count = transaction.execute_update(
            "INSERT Singers (id, first_name, last_name, birth_date, picture) "
            " VALUES {}".format(str(row))
        )
        total_count += count

    if total_count != 99:
        raise ValueError("Wrong number of inserts: " + str(total_count))


def compare_measurements(spanner, alchemy):
    comparison = {}
    for key in spanner.keys():
        comparison[key] = {
            "Spanner, sec": spanner[key],
            "SQLAlchemy, sec": alchemy[key],
            "SQLAlchemy deviation": round(alchemy[key] - spanner[key], 2),
            "SQLAlchemy to Spanner, %": round(alchemy[key] / spanner[key] * 100),
        }
    return comparison


spanner_measures = SpannerBenchmarkTest().run()
alchemy_measures = SQLAlchemyBenchmarkTest().run()

pprint.pprint(compare_measurements(spanner_measures, alchemy_measures))
