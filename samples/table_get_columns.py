# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from sqlalchemy import create_engine, inspect


def get_table_columns(url, table_id):
    """Retrieve the columns list of the table"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_name,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    # [START sqlalchemy_spanner_get_table_columns]
    engine = create_engine(url)
    insp = inspect(engine)
    columns = insp.get_columns(table_id.name)

    print("Columns are:", columns)
    # [END sqlalchemy_spanner_get_table_columns]
    return columns
