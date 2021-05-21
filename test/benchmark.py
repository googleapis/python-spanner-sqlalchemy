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
from google.cloud.spanner_v1 import Client
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
            # self.insert_multiple_rows,
            # self.read_one_row,
            # self.select_multiple_singers,
            # self.select_multiple_singers_in_ReadOnly_transaction,
            # self.select_multiple_singers_in_ReadWrite_transaction,
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

    @measure_execution_time
    def insert_one_row_with_fetch_after(self):
        self._database.run_in_transaction(insert_one_row, self._one_row)

    @measure_execution_time
    def insert_multiple_rows(self):
        pass

    @measure_execution_time
    def read_one_row(self):
        pass

    @measure_execution_time
    def select_multiple_singers(self):
        pass

    @measure_execution_time
    def select_multiple_singers_in_ReadOnly_transaction(self):
        pass

    @measure_execution_time
    def select_multiple_singers_in_ReadWrite_transaction(self):
        pass


class SQLAlchemyBenchmarkTest(BenchmarkTestBase):
    def __init__(self):
        super().__init__()
        engine = create_engine(
            "spanner:///projects/appdev-soda-spanner-staging/instances/"
            "sqlalchemy-dialect-test/databases/compliance-test"
        )
        metadata = MetaData(bind=engine)
        self._table = Table("Singers", metadata, autoload=True)

    @measure_execution_time
    def insert_one_row_with_fetch_after(self):
        insert(self._table).values(self._one_row).execute()
        last_name = (
            select([text("last_name")], from_obj=self._table).execute().fetchone()
        )[0]
        if last_name != "Allison":
            raise ValueError("Received invalid last name: " + last_name)

    @measure_execution_time
    def insert_multiple_rows(self):
        pass

    @measure_execution_time
    def read_one_row(self):
        pass

    @measure_execution_time
    def select_multiple_singers(self):
        pass

    @measure_execution_time
    def select_multiple_singers_in_ReadOnly_transaction(self):
        pass

    @measure_execution_time
    def select_multiple_singers_in_ReadWrite_transaction(self):
        pass


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


def compare_measurements(spanner, alchemy):
    comparison = {}
    for key in spanner.keys():
        comparison[key] = {
            "Spanner, sec": spanner[key],
            "SQLAlchemy, sec": alchemy[key],
            "SQLAlchemy deviation": alchemy[key] - spanner[key],
            "deviation, %": round(alchemy[key] / spanner[key] * 100),
        }
    return comparison


spanner_measures = SpannerBenchmarkTest().run()
alchemy_measures = SQLAlchemyBenchmarkTest().run()

pprint.pprint(compare_measurements(spanner_measures, alchemy_measures))
