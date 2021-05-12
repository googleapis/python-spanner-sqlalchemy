# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from sqlalchemy import create_engine, inspect


def get_table_names(url):
    """Retrieve the list of the table names"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_name,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    # [START sqlalchemy_spanner_get_table_names]
    engine = create_engine(url)
    insp = inspect(engine)
    tables = insp.get_table_names()

    print("Table names are:", tables)
    # [END sqlalchemy_spanner_get_table_names]
    return tables
