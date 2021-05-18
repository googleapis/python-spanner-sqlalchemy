# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from sqlalchemy import create_engine, inspect, Index


def get_table_indexes(url, table_id):
    """Retrieve the Indexes of the table"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_name,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    # Create Index
    index = Index("some_index", table_id.c.user_name)
    index.create()

    # [START sqlalchemy_spanner_get_indexes]
    engine = create_engine(url)
    insp = inspect(engine)
    indexes = insp.get_indexes(table_id.name)

    print("Indexes are:", indexes)
    # [END sqlalchemy_spanner_get_indexes]
    return indexes
