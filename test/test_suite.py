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

from sqlalchemy.testing import config, db
from sqlalchemy.testing import eq_
from sqlalchemy.testing import provide_metadata
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy import literal_column
from sqlalchemy import literal, select, util
from sqlalchemy import String
from sqlalchemy.types import Integer

from sqlalchemy.testing.suite.test_update_delete import *  # noqa: F401, F403
from sqlalchemy.testing.suite.test_dialect import *  # noqa: F401, F403

from sqlalchemy.testing.suite.test_dialect import (  # noqa: F401, F403
    EscapingTest as _EscapingTest,
)

from sqlalchemy.testing.suite.test_types import IntegerTest as _IntegerTest


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


class IntegerTest(_IntegerTest):
    @provide_metadata
    def _round_trip(self, datatype, data):
        metadata = self.metadata
        int_table = Table(
            "integer_table",
            metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("integer_data", Integer),
        )

        metadata.create_all(config.db)

        config.db.execute(int_table.insert(), {"id": 1, "integer_data": data})

        row = config.db.execute(select([int_table.c.integer_data])).first()

        eq_(row, (data,))

        if util.py3k:
            assert isinstance(row[0], int)
        else:
            assert isinstance(row[0], (long, int))  # noqa

        config.db.execute(int_table.delete())

    @provide_metadata
    def _literal_round_trip(self, type_, input_, output, filter_=None):
        """test literal rendering """

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as its
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully
        t = Table("int_t", self.metadata, Column("x", type_))
        t.drop(checkfirst=True)
        t.create()

        with db.connect() as conn:
            for value in input_:
                ins = (
                    t.insert()
                    .values(x=literal(value))
                    .compile(
                        dialect=db.dialect, compile_kwargs=dict(literal_binds=True),
                    )
                )
                conn.execute(ins)

            if self.supports_whereclause:
                stmt = t.select().where(t.c.x == literal(value))
            else:
                stmt = t.select()

            stmt = stmt.compile(
                dialect=db.dialect, compile_kwargs=dict(literal_binds=True),
            )
            for row in conn.execute(stmt):
                value = row[0]
                if filter_ is not None:
                    value = filter_(value)
                assert value in output
            conn.execute(t.delete())
