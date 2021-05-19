# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd


def fetch_rows_with_offset(table):
    """Fetch rows from the table with limit and offset"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_name,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    table.insert().execute([
        {"user_id": 1, "user_name": 'GEH'},
        {"user_id": 2, "user_name": 'DEF'},
        {"user_id": 3, "user_name": 'HIJ'},
        {"user_id": 4, "user_name": 'ABC'}
    ])

    # [START sqlalchemy_spanner_fetch_rows_with_limit_offset]
    result = list(table.select().limit(2).offset(1).execute())
    print("The rows are:", result)
    # [END sqlalchemy_spanner_fetch_rows_with_limit_offset]
    return result
