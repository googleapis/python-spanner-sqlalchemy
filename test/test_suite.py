# -*- coding: utf-8 -*-
#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sqlalchemy.testing.suite.test_dialect import *  # noqa: F401, F403
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import provide_metadata
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy import literal_column
from sqlalchemy import select, case, bindparam
from sqlalchemy import String
from sqlalchemy.testing import requires
from sqlalchemy.types import Integer
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from sqlalchemy.testing.suite.test_update_delete import *  # noqa: F401, F403

from sqlalchemy.testing.suite.test_dialect import (  # noqa: F401, F403
    EscapingTest as _EscapingTest,
)

from sqlalchemy.testing.suite.test_types import (  # noqa: F401, F403
    DateTest as _DateTest,
)

from sqlalchemy.testing.suite.test_types import (  # noqa: F401, F403
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
)

from sqlalchemy.testing.suite.test_types import (  # noqa: F401, F403
    DateTimeTest as _DateTimeTest,
)


class EscapingTest(_EscapingTest):
    @provide_metadata
    def test_percent_sign_round_trip(self):
        """Test that the DBAPI accommodates for escaped / nonescaped
        percent signs in a way that matches the compiler

        SPANNER OVERRIDE
        Cloud Spanner supports tables with empty primary key, but
        only single one row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        m = self.metadata
        t = Table("t", m, Column("data", String(50)))
        t.create(config.db)
        with config.db.begin() as conn:
            conn.execute(t.insert(), dict(data="some % value"))

            eq_(
                conn.scalar(
                    select([t.c.data]).where(
                        t.c.data == literal_column("'some % value'")
                    )
                ),
                "some % value",
            )

            conn.execute(t.delete())
            conn.execute(t.insert(), dict(data="some %% other value"))
            eq_(
                conn.scalar(
                    select([t.c.data]).where(
                        t.c.data == literal_column("'some %% other value'")
                    )
                ),
                "some %% other value",
            )


class DateTest(_DateTest):
    def test_round_trip(self):
        date_table = self.tables.date_table

        config.db.execute(date_table.insert(), {"id": 1, "date_data": self.data})

        row = config.db.execute(select([date_table.c.date_data])).first()

        compare = self.compare or self.data
        eq_(row, (compare,))
        assert isinstance(row[0], type(compare))

    def test_null(self):
        date_table = self.tables.date_table

        config.db.execute(date_table.insert(), {"id": 1, "date_data": None})

        row = config.db.execute(select([date_table.c.date_data])).first()
        eq_(row, (None,))

    @requires.standalone_null_binds_whereclause
    def test_null_bound_comparison(self):
        # this test is based on an Oracle issue observed in #4886.
        # passing NULL for an expression that needs to be interpreted as
        # a certain type, does the DBAPI have the info it needs to do this.
        date_table = self.tables.date_table
        with config.db.connect() as conn:
            result = conn.execute(
                date_table.insert(), {"id": 1, "date_data": self.data}
            )
            id_ = result.inserted_primary_key[0]
            stmt = select([date_table.c.id]).where(
                case(
                    [
                        (
                            bindparam("foo", type_=self.datatype) is not None,
                            bindparam("foo", type_=self.datatype),
                        )
                    ],
                    else_=date_table.c.date_data,
                )
                == date_table.c.date_data
            )

            row = conn.execute(stmt, {"foo": None}).first()
            eq_(row[0], id_)


class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "datetime_table",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("date_data", cls.datatype),
        )

    def test_null(self):
        date_table = self.tables.datetime_table

        config.db.execute(date_table.insert(), {"id": 1, "date_data": None})

        row = config.db.execute(select([date_table.c.date_data])).first()
        eq_(row, (None,))

    def test_round_trip(self):
        date_table = self.tables.datetime_table

        config.db.execute(date_table.insert(), {"id": 1, "date_data": self.data})

        row = config.db.execute(select([date_table.c.date_data])).first()
        compare = self.compare or self.data
        compare = compare.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        eq_(row[0].rfc3339(), compare)
        assert isinstance(row[0], DatetimeWithNanoseconds)

    @requires.standalone_null_binds_whereclause
    def test_null_bound_comparison(self):
        # this test is based on an Oracle issue observed in #4886.
        # passing NULL for an expression that needs to be interpreted as
        # a certain type, does the DBAPI have the info it needs to do this.
        date_table = self.tables.datetime_table
        with config.db.connect() as conn:
            result = conn.execute(
                date_table.insert(), {"id": 1, "date_data": self.data}
            )
            id_ = result.inserted_primary_key[0]
            stmt = select([date_table.c.id]).where(
                case(
                    [
                        (
                            bindparam("foo", type_=self.datatype) is not None,
                            bindparam("foo", type_=self.datatype),
                        )
                    ],
                    else_=date_table.c.date_data,
                )
                == date_table.c.date_data
            )

            row = conn.execute(stmt, {"foo": None}).first()
            eq_(row[0], id_)


class DateTimeTest(_DateTimeTest, DateTimeMicrosecondsTest):
    pass
