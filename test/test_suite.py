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

import pytest

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
    DateTimeHistoricTest,
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
    DateTimeTest as _DateTimeTest,
    TimeTest as _TimeTest,
    TimeMicrosecondsTest as _TimeMicrosecondsTest,
    TimestampMicrosecondsTest,
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
    """Cloud Spanner supports tables with empty primary key, but
    only single one row can be inserted into such a table -
    following insertions will fail with `400 id must not be NULL in table date_table`.
    Overriding the tests and add a manual primary key value to avoid the same failures.
    """

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
                            bindparam("foo", type_=self.datatype) != None,
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
        """Sql alchemy is not able cleanup data and drop the table correctly,
            table was already exists after related tests finished, so it doesn't
            create a new table and when started tests for other data type  following
            insertions will fail with `400 Invalid value for column date_data in
            table date_table: Expected DATE`.
            Overriding the tests to create a new table for test to avoid the same failures.
        """
        Table(
            "datetime_table",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("date_data", cls.datatype),
        )

    def test_null(self):
        """Cloud Spanner supports tables with empty primary key, but
            only single one row can be inserted into such a table -
            following insertions will fail with `400 id must not be NULL in table date_table`.
            Overriding the tests and add a manual primary key value to avoid the same failures.
        """
        date_table = self.tables.datetime_table

        config.db.execute(date_table.insert(), {"id": 1, "date_data": None})

        row = config.db.execute(select([date_table.c.date_data])).first()
        eq_(row, (None,))

    def test_round_trip(self):
        """Cloud Spanner supports tables with empty primary key, but
            only single one row can be inserted into such a table -
            following insertions will fail with `400 id must not be NULL in table date_table`.
            Overriding the tests and add a manual primary key value to avoid the same failures.

            Spanner convert timestamp into `%Y-%m-%dT%H:%M:%S.%fZ` format, so avoid assert
            failures convert datetime input to the desire timestamp format.
        """
        date_table = self.tables.datetime_table
        config.db.execute(date_table.insert(), {"id": 1, "date_data": self.data})

        row = config.db.execute(select([date_table.c.date_data])).first()
        compare = self.compare or self.data
        compare = compare.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        eq_(row[0].rfc3339(), compare)
        assert isinstance(row[0], DatetimeWithNanoseconds)

    @requires.standalone_null_binds_whereclause
    def test_null_bound_comparison(self):
        """Cloud Spanner supports tables with empty primary key, but
            only single one row can be inserted into such a table -
            following insertions will fail with `400 id must not be NULL in table date_table`.
            Overriding the tests and add a manual primary key value to avoid the same failures.
        """
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
                            bindparam("foo", type_=self.datatype) != None,
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
    """
    DateTime class tests have same failures same as DateTimeMicrosecondsTest class tests,
    so to avoid that failures and maintain DRY concept just inherit the class to run tests
    successfully.
    """

    pass


class TimeTests(_TimeMicrosecondsTest, _TimeTest):
    @pytest.mark.skip("Spanner doesn't support Time data type.")
    def test_null(self):
        pass

    @pytest.mark.skip("Spanner doesn't support Time data type.")
    def test_round_trip(self):
        pass

    @pytest.mark.skip("Spanner doesn't support Time data type.")
    @requires.standalone_null_binds_whereclause
    def test_null_bound_comparison(self):
        pass


class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest, TimeTests):
    @pytest.mark.skip("Spanner doesn't date coerces from datetime.")
    def test_null(self):
        pass

    @pytest.mark.skip("Spanner doesn't date coerces from datetime.")
    def test_round_trip(self):
        pass

    @pytest.mark.skip("Spanner doesn't date coerces from datetime.")
    @requires.standalone_null_binds_whereclause
    def test_null_bound_comparison(self):
        pass
