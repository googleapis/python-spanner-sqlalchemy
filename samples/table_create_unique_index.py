# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from sqlalchemy import Index


def create_unique_indexes(table_id):
    """Create unique index"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_name,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    # [START sqlalchemy_spanner_create_unique_indexes]
    index = Index("some_index", table_id.c.user_name, unique=True)
    index.create()
    print("Index created successfully")
    # [END sqlalchemy_spanner_create_unique_indexes]
