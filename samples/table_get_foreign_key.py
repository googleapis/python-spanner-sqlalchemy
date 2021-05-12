# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from sqlalchemy import create_engine, inspect


def get_table_foreign_key(url, table_id):
    """Retrieve the Foreign key of the table"""

    # TODO(developer): Create the table
    # table1 = Table(
    #    table1_name,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table1.create()

    # TODO(developer): Create the table with foreign key
    # table2 = Table(
    #     table2_name,
    #     metadata,
    #     Column("id", Integer, primary_key=True),
    #     Column("name", String(16), nullable=False),
    #     Column(
    #         "table1_id",
    #         Integer,
    #         ForeignKey("table1.id", name="table1id")
    #     ),
    # )
    # table2.create()

    # [START sqlalchemy_spanner_get_foreign_key]
    engine = create_engine(url)
    insp = inspect(engine)
    f_key = insp.get_foreign_keys(table_id.name)

    print("Foreign key is:", f_key)
    # [END sqlalchemy_spanner_get_foreign_key]
    return f_key
