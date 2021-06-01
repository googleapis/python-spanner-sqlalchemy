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

import operator
import pytest
import decimal
import pytz

import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import testing
from sqlalchemy import ForeignKey
from sqlalchemy import MetaData
from sqlalchemy.schema import DDL
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import provide_metadata, emits_warning
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.provision import temp_table_keyword_args
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import util
from sqlalchemy import event
from sqlalchemy import exists
from sqlalchemy import Boolean
from sqlalchemy import Float
from sqlalchemy import LargeBinary
from sqlalchemy import String
from sqlalchemy.types import Integer
from sqlalchemy.types import Numeric
from sqlalchemy.types import Text
from sqlalchemy.testing import requires

from google.api_core.datetime_helpers import DatetimeWithNanoseconds

from google.cloud import spanner_dbapi

from sqlalchemy.testing.suite.test_cte import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_ddl import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_dialect import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_insert import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_reflection import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_results import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_select import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_sequence import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_update_delete import *  # noqa: F401, F403

from sqlalchemy.testing.suite.test_cte import CTETest as _CTETest
from sqlalchemy.testing.suite.test_ddl import TableDDLTest as _TableDDLTest
from sqlalchemy.testing.suite.test_ddl import (
    LongNameBlowoutTest as _LongNameBlowoutTest,
)
from sqlalchemy.testing.suite.test_dialect import EscapingTest as _EscapingTest
from sqlalchemy.testing.suite.test_insert import (
    InsertBehaviorTest as _InsertBehaviorTest,
)
from sqlalchemy.testing.suite.test_select import (  # noqa: F401, F403
    CompoundSelectTest as _CompoundSelectTest,
    ExistsTest as _ExistsTest,
    IsOrIsNotDistinctFromTest as _IsOrIsNotDistinctFromTest,
    LikeFunctionsTest as _LikeFunctionsTest,
    OrderByLabelTest as _OrderByLabelTest,
)
from sqlalchemy.testing.suite.test_reflection import (
    QuotedNameArgumentTest as _QuotedNameArgumentTest,
    ComponentReflectionTest as _ComponentReflectionTest,
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
)
from sqlalchemy.testing.suite.test_results import RowFetchTest as _RowFetchTest
from sqlalchemy.testing.suite.test_types import (  # noqa: F401, F403
    BooleanTest as _BooleanTest,
    DateTest as _DateTest,
    _DateFixture as _DateFixtureTest,
    DateTimeHistoricTest,
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
    DateTimeTest as _DateTimeTest,
    IntegerTest as _IntegerTest,
    _LiteralRoundTripFixture,
    NumericTest as _NumericTest,
    StringTest as _StringTest,
    TextTest as _TextTest,
    TimeTest as _TimeTest,
    TimeMicrosecondsTest as _TimeMicrosecondsTest,
    TimestampMicrosecondsTest,
    UnicodeVarcharTest as _UnicodeVarcharTest,
    UnicodeTextTest as _UnicodeTextTest,
    _UnicodeFixture as _UnicodeFixtureTest,
)

config.test_schema = ""


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


class CTETest(_CTETest):
    @classmethod
    def define_tables(cls, metadata):
        """
        The original method creates a foreign key without a name,
        which causes troubles on test cleanup. Overriding the
        method to explicitly set a foreign key name.
        """
        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            Column("parent_id", ForeignKey("some_table.id", name="fk_some_table")),
        )

        Table(
            "some_other_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            Column("parent_id", Integer),
        )

    @pytest.mark.skip("INSERT from WITH subquery is not supported")
    def test_insert_from_select_round_trip(self):
        """
        The test checks if an INSERT can be done from a cte, like:

        WITH some_cte AS (...)
        INSERT INTO some_other_table (... SELECT * FROM some_cte)

        Such queries are not supported by Spanner.
        """
        pass

    @pytest.mark.skip("DELETE from WITH subquery is not supported")
    def test_delete_scalar_subq_round_trip(self):
        """
        The test checks if a DELETE can be done from a cte, like:

        WITH some_cte AS (...)
        DELETE FROM some_other_table (... SELECT * FROM some_cte)

        Such queries are not supported by Spanner.
        """
        pass

    @pytest.mark.skip("DELETE from WITH subquery is not supported")
    def test_delete_from_round_trip(self):
        """
        The test checks if a DELETE can be done from a cte, like:

        WITH some_cte AS (...)
        DELETE FROM some_other_table (... SELECT * FROM some_cte)

        Such queries are not supported by Spanner.
        """
        pass

    @pytest.mark.skip("UPDATE from WITH subquery is not supported")
    def test_update_from_round_trip(self):
        """
        The test checks if an UPDATE can be done from a cte, like:

        WITH some_cte AS (...)
        UPDATE some_other_table
        SET (... SELECT * FROM some_cte)

        Such queries are not supported by Spanner.
        """
        pass

    @pytest.mark.skip("WITH RECURSIVE subqueries are not supported")
    def test_select_recursive_round_trip(self):
        pass


class BooleanTest(_BooleanTest):
    def test_render_literal_bool(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        self._literal_round_trip(Boolean(), [True], [True])
        self._literal_round_trip(Boolean(), [False], [False])


class ExistsTest(_ExistsTest):
    def test_select_exists(self, connection):
        """
        SPANNER OVERRIDE:

        The original test is trying to execute a query like:

        SELECT ...
        WHERE EXISTS (SELECT ...)

        SELECT WHERE without FROM clause is not supported by Spanner.
        Rewriting the test to force it to generate a query like:

        SELECT EXISTS (SELECT ...)
        """
        stuff = self.tables.stuff
        eq_(
            connection.execute(
                select((exists().where(stuff.c.data == "some data"),))
            ).fetchall(),
            [(True,)],
        )

    def test_select_exists_false(self, connection):
        """
        SPANNER OVERRIDE:

        The original test is trying to execute a query like:

        SELECT ...
        WHERE EXISTS (SELECT ...)

        SELECT WHERE without FROM clause is not supported by Spanner.
        Rewriting the test to force it to generate a query like:

        SELECT EXISTS (SELECT ...)
        """
        stuff = self.tables.stuff
        eq_(
            connection.execute(
                select((exists().where(stuff.c.data == "no data"),))
            ).fetchall(),
            [(False,)],
        )


class TableDDLTest(_TableDDLTest):
    @pytest.mark.skip(
        "Spanner table name must start with an uppercase or lowercase letter"
    )
    def test_underscore_names(self):
        pass


@pytest.mark.skip("Max identifier length in Spanner is 128")
class LongNameBlowoutTest(_LongNameBlowoutTest):
    pass


class DateFixtureTest(_DateFixtureTest):
    @classmethod
    def define_tables(cls, metadata):
        """
        SPANNER OVERRIDE:

        Cloud Spanner doesn't support auto incrementing ids feature,
        which is used by the original test. Overriding the test data
        creation method to disable autoincrement and make id column
        nullable.
        """
        Table(
            "date_table",
            metadata,
            Column("id", Integer, primary_key=True, nullable=True),
            Column("date_data", cls.datatype),
        )


class DateTest(DateFixtureTest, _DateTest):
    """
    SPANNER OVERRIDE:

    DateTest tests used same class method to create table, so to avoid those failures
    and maintain DRY concept just inherit the class to run tests successfully.
    """

    pass


class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest, DateTest):
    def test_round_trip(self):
        """
        SPANNER OVERRIDE:

        Spanner converts timestamp into `%Y-%m-%dT%H:%M:%S.%fZ` format, so to avoid
        assert failures convert datetime input to the desire timestamp format.
        """
        date_table = self.tables.date_table
        config.db.execute(date_table.insert(), {"date_data": self.data})

        row = config.db.execute(select([date_table.c.date_data])).first()
        compare = self.compare or self.data
        compare = compare.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        eq_(row[0].rfc3339(), compare)
        assert isinstance(row[0], DatetimeWithNanoseconds)


class DateTimeTest(_DateTimeTest, DateTimeMicrosecondsTest):
    """
    SPANNER OVERRIDE:

    DateTimeTest tests have the same failures same as DateTimeMicrosecondsTest tests,
    so to avoid those failures and maintain DRY concept just inherit the class to run
    tests successfully.
    """

    pass


@pytest.mark.skip("Spanner doesn't support Time data type.")
class TimeTests(_TimeMicrosecondsTest, _TimeTest):
    pass


@pytest.mark.skip("Spanner doesn't coerce dates from datetime.")
class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest):
    pass


class IntegerTest(_IntegerTest):
    @provide_metadata
    def _round_trip(self, datatype, data):
        """
        SPANNER OVERRIDE:

        This is the helper method for integer class tests which creates a table and
        performs an insert operation.
        Cloud Spanner supports tables with an empty primary key, but only one
        row can be inserted into such a table - following insertions will fail with
        `400 id must not be NULL in table date_table`.
        Overriding the tests and adding a manual primary key value to avoid the same
        failures.
        """
        metadata = self.metadata
        int_table = Table(
            "integer_table",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("integer_data", datatype),
        )

        metadata.create_all(config.db)

        config.db.execute(int_table.insert(), {"id": 1, "integer_data": data})

        row = config.db.execute(select([int_table.c.integer_data])).first()

        eq_(row, (data,))

        if util.py3k:
            assert isinstance(row[0], int)
        else:
            assert isinstance(row[0], (long, int))  # noqa


class UnicodeFixtureTest(_UnicodeFixtureTest):
    @classmethod
    def define_tables(cls, metadata):
        """
        SPANNER OVERRIDE:

        Cloud Spanner doesn't support auto incrementing ids feature,
        which is used by the original test. Overriding the test data
        creation method to disable autoincrement and make id column
        nullable.
        """
        Table(
            "unicode_table",
            metadata,
            Column("id", Integer, primary_key=True, nullable=True),
            Column("unicode_data", cls.datatype),
        )

    def test_round_trip_executemany(self):
        """
        SPANNER OVERRIDE

        Cloud Spanner supports tables with empty primary key, but
        only single one row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            [{"id": i, "unicode_data": self.data} for i in range(3)],
        )

        rows = config.db.execute(select([unicode_table.c.unicode_data])).fetchall()
        eq_(rows, [(self.data,) for i in range(3)])
        for row in rows:
            assert isinstance(row[0], util.text_type)

    @pytest.mark.skip("Spanner doesn't support non-ascii characters")
    def test_literal(self):
        pass

    @pytest.mark.skip("Spanner doesn't support non-ascii characters")
    def test_literal_non_ascii(self):
        pass


class UnicodeVarcharTest(UnicodeFixtureTest, _UnicodeVarcharTest):
    """
    SPANNER OVERRIDE:

    UnicodeVarcharTest class inherits the _UnicodeFixtureTest class's tests,
    so to avoid those failures and maintain DRY concept just inherit the class to run
    tests successfully.
    """

    pass


class UnicodeTextTest(UnicodeFixtureTest, _UnicodeTextTest):
    """
    SPANNER OVERRIDE:

    UnicodeTextTest class inherits the _UnicodeFixtureTest class's tests,
    so to avoid those failures and maintain DRY concept just inherit the class to run
    tests successfully.
    """

    pass


class ComponentReflectionTest(_ComponentReflectionTest):
    @classmethod
    def define_temp_tables(cls, metadata):
        """
        SPANNER OVERRIDE:

        In Cloud Spanner unique indexes are used instead of directly
        creating unique constraints. Overriding the test to replace
        constraints with indexes in testing data.
        """
        kw = temp_table_keyword_args(config, config.db)
        user_tmp = Table(
            "user_tmp",
            metadata,
            Column("id", sqlalchemy.INT, primary_key=True),
            Column("name", sqlalchemy.VARCHAR(50)),
            Column("foo", sqlalchemy.INT),
            sqlalchemy.Index("user_tmp_uq", "name", unique=True),
            sqlalchemy.Index("user_tmp_ix", "foo"),
            **kw
        )
        if (
            testing.requires.view_reflection.enabled
            and testing.requires.temporary_views.enabled
        ):
            event.listen(
                user_tmp,
                "after_create",
                DDL("create temporary view user_tmp_v as " "select * from user_tmp"),
            )
            event.listen(user_tmp, "before_drop", DDL("drop view user_tmp_v"))

    @testing.provide_metadata
    def _test_get_unique_constraints(self, schema=None):
        """
        SPANNER OVERRIDE:

        In Cloud Spanner unique indexes are used instead of directly
        creating unique constraints. Overriding the test to replace
        constraints with indexes in testing data.
        """
        # SQLite dialect needs to parse the names of the constraints
        # separately from what it gets from PRAGMA index_list(), and
        # then matches them up.  so same set of column_names in two
        # constraints will confuse it.    Perhaps we should no longer
        # bother with index_list() here since we have the whole
        # CREATE TABLE?
        uniques = sorted(
            [
                {"name": "unique_a", "column_names": ["a"]},
                {"name": "unique_a_b_c", "column_names": ["a", "b", "c"]},
                {"name": "unique_c_a_b", "column_names": ["c", "a", "b"]},
                {"name": "unique_asc_key", "column_names": ["asc", "key"]},
                {"name": "i.have.dots", "column_names": ["b"]},
                {"name": "i have spaces", "column_names": ["c"]},
            ],
            key=operator.itemgetter("name"),
        )
        orig_meta = self.metadata
        Table(
            "testtbl",
            orig_meta,
            Column("a", sqlalchemy.String(20)),
            Column("b", sqlalchemy.String(30)),
            Column("c", sqlalchemy.Integer),
            # reserved identifiers
            Column("asc", sqlalchemy.String(30)),
            Column("key", sqlalchemy.String(30)),
            sqlalchemy.Index("unique_a", "a", unique=True),
            sqlalchemy.Index("unique_a_b_c", "a", "b", "c", unique=True),
            sqlalchemy.Index("unique_c_a_b", "c", "a", "b", unique=True),
            sqlalchemy.Index("unique_asc_key", "asc", "key", unique=True),
            schema=schema,
        )
        orig_meta.create_all()

        inspector = inspect(orig_meta.bind)
        reflected = sorted(
            inspector.get_unique_constraints("testtbl", schema=schema),
            key=operator.itemgetter("name"),
        )

        names_that_duplicate_index = set()

        for orig, refl in zip(uniques, reflected):
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            dupe = refl.pop("duplicates_index", None)
            if dupe:
                names_that_duplicate_index.add(dupe)
            eq_(orig, refl)

        reflected_metadata = MetaData()
        reflected = Table(
            "testtbl", reflected_metadata, autoload_with=orig_meta.bind, schema=schema,
        )

        # test "deduplicates for index" logic.   MySQL and Oracle
        # "unique constraints" are actually unique indexes (with possible
        # exception of a unique that is a dupe of another one in the case
        # of Oracle).  make sure # they aren't duplicated.
        idx_names = set([idx.name for idx in reflected.indexes])
        uq_names = set(
            [
                uq.name
                for uq in reflected.constraints
                if isinstance(uq, sqlalchemy.UniqueConstraint)
            ]
        ).difference(["unique_c_a_b"])

        assert not idx_names.intersection(uq_names)
        if names_that_duplicate_index:
            eq_(names_that_duplicate_index, idx_names)
            eq_(uq_names, set())

    @testing.provide_metadata
    def test_unique_constraint_raises(self):
        """
        Checking that unique constraint creation
        fails due to a ProgrammingError.
        """
        Table(
            "user_tmp_failure",
            self.metadata,
            Column("id", sqlalchemy.INT, primary_key=True),
            Column("name", sqlalchemy.VARCHAR(50)),
            sqlalchemy.UniqueConstraint("name", name="user_tmp_uq"),
        )

        with pytest.raises(spanner_dbapi.exceptions.ProgrammingError):
            self.metadata.create_all()

    @testing.provide_metadata
    def _test_get_table_names(self, schema=None, table_type="table", order_by=None):
        """
        SPANNER OVERRIDE:

        Spanner doesn't support temporary tables, so real tables are
        used for testing. As the original test expects only real
        tables to be read, and in Spanner all the tables are real,
        expected results override is required.
        """
        _ignore_tables = [
            "comment_test",
            "noncol_idx_test_pk",
            "noncol_idx_test_nopk",
            "local_table",
            "remote_table",
            "remote_table_2",
        ]
        meta = self.metadata

        insp = inspect(meta.bind)

        if table_type == "view":
            table_names = insp.get_view_names(schema)
            table_names.sort()
            answer = ["email_addresses_v", "users_v"]
            eq_(sorted(table_names), answer)
        else:
            if order_by:
                tables = [
                    rec[0]
                    for rec in insp.get_sorted_table_and_fkc_names(schema)
                    if rec[0]
                ]
            else:
                tables = insp.get_table_names(schema)
            table_names = [t for t in tables if t not in _ignore_tables]

            if order_by == "foreign_key":
                answer = {"dingalings", "email_addresses", "user_tmp", "users"}
                eq_(set(table_names), answer)
            else:
                answer = ["dingalings", "email_addresses", "user_tmp", "users"]
                eq_(sorted(table_names), answer)

    @pytest.mark.skip("Spanner doesn't support temporary tables")
    def test_get_temp_table_indexes(self):
        pass

    @pytest.mark.skip("Spanner doesn't support temporary tables")
    def test_get_temp_table_unique_constraints(self):
        pass

    @testing.requires.table_reflection
    def test_numeric_reflection(self):
        """
        SPANNER OVERRIDE:

        Spanner defines NUMERIC type with the constant precision=38
        and scale=9. Overriding the test to check if the NUMERIC
        column is successfully created and has dimensions
        correct for Cloud Spanner.
        """
        for typ in self._type_round_trip(Numeric(18, 5)):
            assert isinstance(typ, Numeric)
            eq_(typ.precision, 38)
            eq_(typ.scale, 9)

    @testing.requires.table_reflection
    def test_binary_reflection(self):
        """
        Check that a BYTES column with an explicitly
        set size is correctly reflected.
        """
        for typ in self._type_round_trip(LargeBinary(20)):
            assert isinstance(typ, LargeBinary)
            eq_(typ.length, 20)


class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    @testing.requires.foreign_key_constraint_reflection
    @testing.provide_metadata
    def test_fk_column_order(self):
        """
        SPANNER OVERRIDE:

        Spanner column usage reflection doesn't support determenistic
        ordering. Overriding the test to check that columns are
        reflected correctly, without considering their order.
        """
        # test for issue #5661
        meta = self.metadata
        insp = inspect(meta.bind)
        foreign_keys = insp.get_foreign_keys(self.tables.tb2.name)
        eq_(len(foreign_keys), 1)
        fkey1 = foreign_keys[0]
        eq_(set(fkey1.get("referred_columns")), {"name", "id", "attr"})
        eq_(set(fkey1.get("constrained_columns")), {"pname", "pid", "pattr"})


class RowFetchTest(_RowFetchTest):
    def test_row_w_scalar_select(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner returns a DatetimeWithNanoseconds() for date
        data types. Overriding the test to use a DatetimeWithNanoseconds
        type value as an expected result.
        --------------

        test that a scalar select as a column is returned as such
        and that type conversion works OK.

        (this is half a SQLAlchemy Core test and half to catch database
        backends that may have unusual behavior with scalar selects.)
        """
        datetable = self.tables.has_dates
        s = select([datetable.alias("x").c.today]).as_scalar()
        s2 = select([datetable.c.id, s.label("somelabel")])
        row = config.db.execute(s2).first()

        eq_(
            row["somelabel"],
            DatetimeWithNanoseconds(2006, 5, 12, 12, 0, 0, tzinfo=pytz.UTC),
        )


class InsertBehaviorTest(_InsertBehaviorTest):
    @pytest.mark.skip("Spanner doesn't support empty inserts")
    def test_empty_insert(self):
        pass

    @pytest.mark.skip("Spanner doesn't support auto increment")
    def test_insert_from_select_autoinc(self):
        pass

    @pytest.mark.skip("Spanner doesn't support default column values")
    def test_insert_from_select_with_defaults(self):
        pass

    def test_autoclose_on_insert(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner doesn't support tables with an auto increment primary key,
        following insertions will fail with `400 id must not be NULL in table
        autoinc_pk`.

        Overriding the tests and adding a manual primary key value to avoid the same
        failures.
        """
        if config.requirements.returning.enabled:
            engine = engines.testing_engine(options={"implicit_returning": False})
        else:
            engine = config.db

        with engine.begin() as conn:
            r = conn.execute(
                self.tables.autoinc_pk.insert(), dict(id=1, data="some data")
            )

        assert r._soft_closed
        assert not r.closed
        assert r.is_insert
        assert not r.returns_rows


class BytesTest(_LiteralRoundTripFixture, fixtures.TestBase):
    __backend__ = True

    def test_nolength_binary(self):
        metadata = MetaData()
        foo = Table("foo", metadata, Column("one", LargeBinary))

        foo.create(config.db)
        foo.drop(config.db)


class StringTest(_StringTest):
    @pytest.mark.skip("Spanner doesn't support non-ascii characters")
    def test_literal_non_ascii(self):
        pass


class TextTest(_TextTest):
    @classmethod
    def define_tables(cls, metadata):
        """
        SPANNER OVERRIDE:

        Cloud Spanner doesn't support auto incrementing ids feature,
        which is used by the original test. Overriding the test data
        creation method to disable autoincrement and make id column
        nullable.
        """
        Table(
            "text_table",
            metadata,
            Column("id", Integer, primary_key=True, nullable=True),
            Column("text_data", Text),
        )

    @pytest.mark.skip("Spanner doesn't support non-ascii characters")
    def test_literal_non_ascii(self):
        pass


class NumericTest(_NumericTest):
    @emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_render_literal_numeric(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        self._literal_round_trip(
            Numeric(precision=8, scale=4), [15.7563], [decimal.Decimal("15.7563")],
        )
        self._literal_round_trip(
            Numeric(precision=8, scale=4),
            [decimal.Decimal("15.7563")],
            [decimal.Decimal("15.7563")],
        )

    @emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_render_literal_numeric_asfloat(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        self._literal_round_trip(
            Numeric(precision=8, scale=4, asdecimal=False), [15.7563], [15.7563],
        )
        self._literal_round_trip(
            Numeric(precision=8, scale=4, asdecimal=False),
            [decimal.Decimal("15.7563")],
            [15.7563],
        )

    def test_render_literal_float(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        self._literal_round_trip(
            Float(4),
            [decimal.Decimal("15.7563")],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )

        self._literal_round_trip(
            Float(4),
            [decimal.Decimal("15.7563")],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )

    @requires.precision_generic_float_type
    def test_float_custom_scale(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        self._do_test(
            Float(None, decimal_return_scale=7, asdecimal=True),
            [15.7563827],
            [decimal.Decimal("15.7563827")],
            check_scale=True,
        )

        self._do_test(
            Float(None, decimal_return_scale=7, asdecimal=True),
            [15.7563827],
            [decimal.Decimal("15.7563827")],
            check_scale=True,
        )

    def test_numeric_as_decimal(self):
        """
        SPANNER OVERRIDE:

        Spanner throws an error 400 Value has type FLOAT64 which cannot be
        inserted into column x, which has type NUMERIC for value 15.7563.
        Overriding the test to remove the failure case.
        """
        self._do_test(
            Numeric(precision=8, scale=4),
            [decimal.Decimal("15.7563")],
            [decimal.Decimal("15.7563")],
        )

    def test_numeric_as_float(self):
        """
        SPANNER OVERRIDE:

        Spanner throws an error 400 Value has type FLOAT64 which cannot be
        inserted into column x, which has type NUMERIC for value 15.7563.
        Overriding the test to remove the failure case.
        """

        self._do_test(
            Numeric(precision=8, scale=4, asdecimal=False),
            [decimal.Decimal("15.7563")],
            [15.7563],
        )

    @requires.floats_to_four_decimals
    def test_float_as_decimal(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        self._do_test(
            Float(precision=8, asdecimal=True), [15.7563], [decimal.Decimal("15.7563")],
        )

        self._do_test(
            Float(precision=8, asdecimal=True),
            [decimal.Decimal("15.7563")],
            [decimal.Decimal("15.7563")],
        )

    def test_float_as_float(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """
        self._do_test(
            Float(precision=8),
            [15.7563],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )

        self._do_test(
            Float(precision=8),
            [decimal.Decimal("15.7563")],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )

    @requires.precision_numerics_general
    def test_precision_decimal(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.

        Remove an extra digits after decimal point as cloud spanner is
        capable of representing an exact numeric value with a precision
        of 38 and scale of 9.
        """
        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("54.234246451")],
            [decimal.Decimal("54.234246451")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("0.004354")],
            [decimal.Decimal("0.004354")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("900.0")],
            [decimal.Decimal("900.0")],
        )

    @testing.requires.precision_numerics_enotation_large
    def test_enotation_decimal_large(self):
        """test exceedingly large decimals.

        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.
        """

        self._do_test(
            Numeric(precision=25, scale=2),
            [decimal.Decimal("4E+8")],
            [decimal.Decimal("4E+8")],
        )

        self._do_test(
            Numeric(precision=25, scale=2),
            [decimal.Decimal("5748E+15")],
            [decimal.Decimal("5748E+15")],
        )

        self._do_test(
            Numeric(precision=25, scale=2),
            [decimal.Decimal("1.521E+15")],
            [decimal.Decimal("1.521E+15")],
        )

        self._do_test(
            Numeric(precision=25, scale=2),
            [decimal.Decimal("00000000000000.1E+12")],
            [decimal.Decimal("00000000000000.1E+12")],
        )

    @testing.requires.precision_numerics_enotation_large
    def test_enotation_decimal(self):
        """test exceedingly small decimals.

        Decimal reports values with E notation when the exponent
        is greater than 6.

        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.

        Remove extra digits after decimal point as cloud spanner is
        capable of representing an exact numeric value with a precision
        of 38 and scale of 9.
        """
        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("1E-2")],
            [decimal.Decimal("1E-2")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("1E-3")],
            [decimal.Decimal("1E-3")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("1E-4")],
            [decimal.Decimal("1E-4")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("1E-5")],
            [decimal.Decimal("1E-5")],
        )

        self._do_test(
            Numeric(precision=18, scale=14),
            [decimal.Decimal("1E-6")],
            [decimal.Decimal("1E-6")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("1E-7")],
            [decimal.Decimal("1E-7")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("1E-8")],
            [decimal.Decimal("1E-8")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("0.010000059")],
            [decimal.Decimal("0.010000059")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("0.000000059")],
            [decimal.Decimal("0.000000059")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("0.000000696")],
            [decimal.Decimal("0.000000696")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("0.700000696")],
            [decimal.Decimal("0.700000696")],
        )

        self._do_test(
            Numeric(precision=18, scale=9),
            [decimal.Decimal("696E-9")],
            [decimal.Decimal("696E-9")],
        )


class LikeFunctionsTest(_LikeFunctionsTest):
    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_contains_autoescape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_contains_autoescape_escape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_contains_escape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_endswith_autoescape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_endswith_escape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_endswith_autoescape_escape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_startswith_autoescape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_startswith_escape(self):
        pass

    @pytest.mark.skip("Spanner doesn't support LIKE ESCAPE clause")
    def test_startswith_autoescape_escape(self):
        pass

    def test_escape_keyword_raises(self):
        """Check that ESCAPE keyword causes an exception when used."""
        with pytest.raises(NotImplementedError):
            col = self.tables.some_table.c.data
            self._test(col.contains("b##cde", escape="#"), {7})


@pytest.mark.skip("Spanner doesn't support quotes in table names.")
class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    pass


@pytest.mark.skip("Spanner doesn't support IS DISTINCT FROM clause")
class IsOrIsNotDistinctFromTest(_IsOrIsNotDistinctFromTest):
    pass


class OrderByLabelTest(_OrderByLabelTest):
    @pytest.mark.skip(
        "Spanner requires an alias for the GROUP BY list when specifying derived "
        "columns also used in SELECT"
    )
    def test_group_by_composed(self):
        pass


class CompoundSelectTest(_CompoundSelectTest):
    """
    See: https://github.com/googleapis/python-spanner/issues/347
    """

    @pytest.mark.skip(
        "Spanner DBAPI incorrectly classify the statement starting with brackets."
    )
    def test_limit_offset_selectable_in_unions(self):
        pass

    @pytest.mark.skip(
        "Spanner DBAPI incorrectly classify the statement starting with brackets."
    )
    def test_order_by_selectable_in_unions(self):
        pass


class JoinTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "students",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("lastname", String),
        )

        Table(
            "addresses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("st_id", Integer, ForeignKey("students.id")),
            Column("postal_add", String),
            Column("email_add", String),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.students.insert(),
            [
                {"id": 1, "name": "Ravi", "lastname": "Kapoor"},
                {"id": 2, "name": "Rajiv", "lastname": "Khanna"},
                {"id": 3, "name": "Komal", "lastname": "Bhandari"},
                {"id": 4, "name": "Abdul", "lastname": "Sattar"},
                {"id": 5, "name": "Priya", "lastname": "Rajhans"},
            ],
        )

        connection.execute(
            cls.tables.addresses.insert(),
            [
                {
                    "id": 1,
                    "st_id": 1,
                    "postal_add": "Shivajinagar Pune",
                    "email_add": "ravi@gmail.com",
                },
                {
                    "id": 2,
                    "st_id": 1,
                    "postal_add": "ChurchGate Mumbai",
                    "email_add": "kapoor@gmail.com",
                },
                {
                    "id": 3,
                    "st_id": 3,
                    "postal_add": "Jubilee Hills Hyderabad",
                    "email_add": " komal@gmail.com",
                },
                {
                    "id": 4,
                    "st_id": 5,
                    "postal_add": "MG Road Bangaluru",
                    "email_add": "as@yahoo.com",
                },
                {
                    "id": 5,
                    "st_id": 2,
                    "postal_add": "Cannought Place new Delhi",
                    "email_add": "admin@khanna.com",
                },
            ],
        )

    def test_inner_join(self):
        students = self.tables.students
        addresses = self.tables.addresses
        stmt = select([students]).select_from(
            students.join(addresses, students.c.id == addresses.c.st_id)
        )

        output = [
            (1, "Ravi", "Kapoor"),
            (1, "Ravi", "Kapoor"),
            (2, "Rajiv", "Khanna"),
            (3, "Komal", "Bhandari"),
            (5, "Priya", "Rajhans"),
        ]

        eq_(config.db.execute(stmt).fetchall(), output)

    def test_left_outer_join(self):
        students = self.tables.students
        addresses = self.tables.addresses

        j = students.outerjoin(addresses)
        stmt = select([students]).select_from(j)

        output = [
            (1, "Ravi", "Kapoor"),
            (1, "Ravi", "Kapoor"),
            (3, "Komal", "Bhandari"),
            (5, "Priya", "Rajhans"),
            (2, "Rajiv", "Khanna"),
            (4, "Abdul", "Sattar"),
        ]

        eq_(config.db.execute(stmt).fetchall(), output)
