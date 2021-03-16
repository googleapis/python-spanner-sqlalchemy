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
import decimal

from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import provide_metadata, emits_warning, requires
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy import literal_column
from sqlalchemy import exists
from sqlalchemy import select
from sqlalchemy import Boolean, Float, Numeric, literal
from sqlalchemy import String

from sqlalchemy.testing.suite.test_ddl import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_cte import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_dialect import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_update_delete import *  # noqa: F401, F403

from sqlalchemy.testing.suite.test_cte import CTETest as _CTETest
from sqlalchemy.testing.suite.test_ddl import TableDDLTest as _TableDDLTest
from sqlalchemy.testing.suite.test_ddl import (
    LongNameBlowoutTest as _LongNameBlowoutTest,
)

from sqlalchemy.testing.suite.test_dialect import EscapingTest as _EscapingTest
from sqlalchemy.testing.suite.test_select import ExistsTest as _ExistsTest
from sqlalchemy.testing.suite.test_types import BooleanTest as _BooleanTest
from sqlalchemy.testing.suite.test_types import NumericTest as _NumericTest


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


class NumericTest(_NumericTest):
    @provide_metadata
    def _literal_round_trip(self, type_, input_, output, filter_=None):
        """
        SPANNER OVERRIDE:

        Spanner is not able cleanup data and drop the table correctly,
        table was already exists after related tests finished, so it doesn't
        create a new table and when started tests for other data type  following
        insertions will fail with `400 Duplicate name in schema: t.
        Overriding the tests to create a new table for test and drop table manually
        before it creates a new table to avoid the same failures."""

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as its
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully
        t = Table("t", self.metadata, Column("x", type_))
        t.drop(checkfirst=True)
        t.create()

        with config.db.connect() as conn:
            config.db.execute(t.delete())
            for value in input_:
                ins = (
                    t.insert()
                    .values(x=literal(value))
                    .compile(
                        dialect=config.db.dialect,
                        compile_kwargs=dict(literal_binds=True),
                    )
                )
                conn.execute(ins)

            if self.supports_whereclause:
                stmt = t.select().where(t.c.x == literal(value))
            else:
                stmt = t.select()

            stmt = stmt.compile(
                dialect=config.db.dialect, compile_kwargs=dict(literal_binds=True),
            )
            for row in conn.execute(stmt):
                value = row[0]
                if filter_ is not None:
                    value = filter_(value)
                assert value in output

    @emits_warning(r".*does \*not\* support Decimal objects natively")
    @provide_metadata
    def _do_test(self, type_, input_, output, filter_=None, check_scale=False):
        """
        SPANNER OVERRIDE:

        Spanner is not able cleanup data and drop the table correctly,
        table was already exists after related tests finished, so it doesn't
        create a new table and when started tests for other data type  following
        insertions will fail with `400 Duplicate name in schema: t.
        Overriding the tests to create a new table for test and drop table manually
        before it creates a new table to avoid the same failures.
        """
        metadata = self.metadata
        t = Table("t_decimal", metadata, Column("x", type_))
        t.drop(checkfirst=True)
        t.create()
        t.insert().execute([{"x": x} for x in input_])

        result = {row[0] for row in t.select().execute()}
        output = set(output)
        if filter_:
            result = set(filter_(x) for x in result)
            output = set(filter_(x) for x in output)
        eq_(result, output)

        if check_scale:
            eq_([str(x) for x in result], [str(x) for x in output])

    @pytest.mark.skip(
        "Dialect spanner+spanner does *not* support Decimal objects natively, "
        "and SQLAlchemy must convert from floating point - rounding errors and "
        "other issues may occur"
    )
    def test_decimal_coerce_round_trip_w_cast(self):
        pass

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
            Numeric(precision=8, scale=4, asdecimal=False),
            [15.7563],
            [decimal.Decimal("15.7563")],
        )
        self._literal_round_trip(
            Numeric(precision=8, scale=4, asdecimal=False),
            [decimal.Decimal("15.7563")],
            [decimal.Decimal("15.7563")],
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

    @pytest.mark.skip("It's flaky test, due to drop table issue.")
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
            [decimal.Decimal("15.7563827")],
            [decimal.Decimal("15.7563827")],
            check_scale=True,
        )

    @pytest.mark.skip("Spanner doesn't support")
    def test_numeric_as_decimal(self):
        """Spanner throws an error `400 Invalid value for column x in
        table t_decimal: Expected NUMERIC` when insert a decimal data
        in numeric type column"""
        pass

    @pytest.mark.skip("Spanner doesn't support")
    def test_numeric_as_float(self):
        """Spanner throws an error `400 Invalid value for column x in
        table t_decimal: Expected NUMERIC` when insert a decimal data
        in numeric type column"""
        pass

    @pytest.mark.skip("It's flaky test, due to drop table issue.")
    @requires.floats_to_four_decimals
    def test_float_as_decimal(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.

        For now it's flaky test due to drop table issue and raise
        the error in alternate execution due to manually dropping the table.
        """
        self._do_test(
            Float(precision=8, asdecimal=True), [15.7563], [decimal.Decimal("15.7563")],
        )
        self._do_test(
            Float(precision=8, asdecimal=True),
            [decimal.Decimal("15.7563")],
            [decimal.Decimal("15.7563")],
        )

    @pytest.mark.skip("It's flaky test, due to drop table issue.")
    def test_float_as_float(self):
        """
        SPANNER OVERRIDE:

        Cloud Spanner supports tables with an empty primary key, but
        only a single row can be inserted into such a table -
        following insertions will fail with `Row [] already exists".
        Overriding the test to avoid the same failure.

        For now it's flaky test due to drop table issue and raise
        the error in alternate execution due to manually dropping the table.
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

    @pytest.mark.skip("Spanner doesn't support")
    @requires.precision_numerics_general
    def test_precision_decimal(self):
        """Spanner throws an error `400 Invalid value for column x in
        table t_decimal: Expected NUMERIC` when insert a decimal data
        in numeric type column"""
        pass
