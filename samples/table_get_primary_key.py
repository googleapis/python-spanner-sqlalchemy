# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from sqlalchemy import create_engine, inspect


def get_table_primary_key(url, table_id):
    """Retrieve the primary key of the table"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_id,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    # [START sqlalchemy_spanner_get_primary_key]
    engine = create_engine(url)
    insp = inspect(engine)
    p_key = insp.get_pk_constraint(table_id.name)
    
    print("Primary key is:", p_key)
    # [END sqlalchemy_spanner_get_primary_key]
    return p_key
