# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bs

"""This application demonstrates how to do basic operations with Cloud
Spanner database.
For more information, see the README.md under /python-spanner-sqlalchemy.
"""

from sqlalchemy import (
    Column,
    create_engine,
    Index,
    Integer,
    inspect,
    MetaData,
    String,
    Table,
)


# [START sqlalchemy_spanner_autocommit_on]
def enable_autocommit_mode(url):
    """Enables autocommit mode."""
    conn = create_engine(url).connect()
    level = conn.get_isolation_level()

    print("Connection autocommit default mode is {}".format(level))
    print(
        "Spanner DBAPI default autocommit mode is {}".format(
            conn.connection.connection.autocommit
        )
    )

    conn.execution_options(isolation_level="AUTOCOMMIT")
    print("Connection autocommit mode is {}".format(level))
    print(
        "Spanner DBAPI autocommit mode is {}".format(
            conn.connection.connection.autocommit
        )
    )
    return conn


# [END sqlalchemy_spanner_autocommit_on]


# [START sqlalchemy_spanner_create_table]
def create_table(url, random_table_id):
    """Create the table."""
    engine = create_engine(url)
    metadata = MetaData(bind=engine)

    table = Table(
        random_table_id,
        metadata,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
    )
    table.create()

    print("Table {} is created successfully".format(table.name))
    return table


# [END sqlalchemy_spanner_create_table]


# [START sqlalchemy_spanner_drop_table]
def drop_table(table):
    """Drop the table."""
    table.drop()

    print("Table {} is dropped successfully".format(table.name))


# [END sqlalchemy_spanner_drop_table]


# [START sqlalchemy_spanner_get_table_names]
def get_table_names(url):
    """Retrieve the list of the table names.

    The table must already exist and can be created using
    `create_table.`
    """
    engine = create_engine(url)
    insp = inspect(engine)
    tables = insp.get_table_names()

    print("Table names are:", tables)
    return tables


# [END sqlalchemy_spanner_get_table_names]


# [START sqlalchemy_spanner_create_unique_indexes]
def create_unique_indexes(table):
    """Create unique index.

    The table must already exist and can be created using
    `create_table.`
    """
    index = Index("some_index", table.c.user_name, unique=True)
    index.create()
    print("Index created successfully")


# [END sqlalchemy_spanner_create_unique_indexes]


# [START sqlalchemy_spanner_delete_all_rows]
def delete_all_rows(table):
    """Delete all rows from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = table.select().execute().fetchall()
    print("Total inserted rows:", len(result))

    table.delete().execute()

    result = table.select().execute().fetchall()
    print("Total rows:", len(result))


# [END sqlalchemy_spanner_delete_all_rows]


# [START sqlalchemy_spanner_delete_row_with_where_condition]
def delete_row_with_where_condition(table):
    """Delete selected row from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = table.select().execute().fetchall()
    print("Total inserted rows:", len(result))

    table.delete().where(table.c.user_id == 1).execute()

    result = table.select().execute().fetchall()
    print("Total rows:", len(result))


# [END sqlalchemy_spanner_delete_row_with_where_condition]


# [START sqlalchemy_spanner_table_exists]
def table_exists(table):
    """Check the table exists.

    The table must already exist and can be created using
    `create_table.`
    """
    result = table.exists()
    print("Table exists:", result)


# [END sqlalchemy_spanner_table_exists]


# [START sqlalchemy_spanner_fetch_row_with_where_condition]
def fetch_row_with_where_condition(table):
    """Fetch row with where condition from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().where(table.c.user_id == 1).execute())

    print("Output is :", result)
    return result


# [END sqlalchemy_spanner_fetch_row_with_where_condition]


# [START sqlalchemy_spanner_fetch_rows]
def fetch_rows(table):
    """Fetch all rows from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = table.select().execute().fetchall()
    print("Total rows:", result)
    return result


# [END sqlalchemy_spanner_fetch_rows]


# [START sqlalchemy_spanner_fetch_rows_with_limit]
def fetch_rows_with_limit(table):
    """Fetch rows from the table with limit.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().limit(2).execute())
    print("The rows are:", result)
    return result


# [END sqlalchemy_spanner_fetch_rows_with_limit]


# [START sqlalchemy_spanner_fetch_rows_with_limit_offset]
def fetch_rows_with_limit_offset(table):
    """Fetch rows from the table with limit and offset.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().limit(2).offset(1).execute())
    print("The rows are:", result)
    return result


# [END sqlalchemy_spanner_fetch_rows_with_limit_offset]


# [START sqlalchemy_spanner_fetch_rows_with_offset]
def fetch_rows_with_offset(table):
    """Fetch rows from the table with offset.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().offset(2).execute())
    print("The rows are:", result)
    return result


# [END sqlalchemy_spanner_fetch_rows_with_offset]


# [START sqlalchemy_spanner_fetch_rows_with_order_by]
def fetch_rows_with_order_by(table):
    """Fetch all rows from the table in order.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().order_by(table.c.user_name).execute().fetchall())
    print("The order by result is:", result)


# [END sqlalchemy_spanner_fetch_rows_with_order_by]


# [START sqlalchemy_spanner_filter_data_endswith]
def filter_data_endswith(table):
    """Filter data with endswith from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().where(table.c.user_name.endswith("%efg")).execute())
    print("Filtered data:", result)
    return result


# [END sqlalchemy_spanner_filter_data_endswith]


# [START sqlalchemy_spanner_filter_data_startswith]
def filter_data_startswith(table):
    """Filter data with startswith from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().where(table.c.user_name.startswith("abcd%")).execute())
    print("Filtered data:", result)
    return result


# [END sqlalchemy_spanner_filter_data_startswith]


# [START sqlalchemy_spanner_filter_data_with_contains]
def filter_data_with_contains(table):
    """Filter data with contains from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().where(table.c.user_name.contains("defg")).execute())
    print("Filtered data:", result)
    return result


# [END sqlalchemy_spanner_filter_data_with_contains]


# [START sqlalchemy_spanner_filter_data_with_like]
def filter_data_with_like(table):
    """Filter data with like from the table.

    The table must already exist and can be created using
    `create_table.`
    """
    result = list(table.select().where(table.c.user_name.like("abc%")).execute())
    print("Filtered data:", result)
    return result


# [END sqlalchemy_spanner_filter_data_with_like]


# [START sqlalchemy_spanner_get_table_columns]
def get_table_columns(url, table):
    """Retrieve the columns list of the table.

    The table must already exist and can be created using
    `create_table.`
    """
    engine = create_engine(url)
    insp = inspect(engine)
    columns = insp.get_columns(table.name)

    print("Columns are:", columns)
    return columns


# [END sqlalchemy_spanner_get_table_columns]


# [START sqlalchemy_spanner_get_foreign_key]
def get_table_foreign_key(url, table):
    """Retrieve the Foreign key of the table.

    The table must already exist and can be created using
    `create_table.`
    """
    engine = create_engine(url)
    insp = inspect(engine)
    f_key = insp.get_foreign_keys(table.name)

    print("Foreign key is:", f_key)
    return f_key


# [END sqlalchemy_spanner_get_foreign_key]


# [START sqlalchemy_spanner_get_indexes]
def get_table_indexes(url, table):
    """Retrieve the Indexes of the table.

    The table must already exist and can be created using
    `create_table.`
    """
    # Create Index
    index = Index("some_index", table.c.user_name)
    index.create()

    engine = create_engine(url)
    insp = inspect(engine)
    indexes = insp.get_indexes(table.name)

    print("Indexes are:", indexes)
    return indexes


# [END sqlalchemy_spanner_get_indexes]


# [START sqlalchemy_spanner_get_primary_key]
def get_table_primary_key(url, table):
    """Retrieve the Primary key of the table.

    The table must already exist and can be created using
    `create_table.`
    """
    engine = create_engine(url)
    insp = inspect(engine)
    p_key = insp.get_pk_constraint(table.name)

    print("Primary key is:", p_key)
    return p_key


# [END sqlalchemy_spanner_get_primary_key]


# [START sqlalchemy_spanner_insert_row]
def insert_row(table):
    """Insert row in the table.

    The table must already exist and can be created using
    `create_table.`
    """
    table.insert().execute({"user_id": 1, "user_name": "ABC"})

    result = [row for row in table.select().execute()]

    print("Total rows:", result)
    return result


# [END sqlalchemy_spanner_insert_row]


# [START sqlalchemy_spanner_update_row]
def update_row(table):
    """Update row in the table.

    The table must already exist and can be created using
    `create_table.`
    """
    table.update().where(table.c.user_id == 2).values(user_name="GEH").execute()
    result = list(table.select().where(table.c.user_id == 2).execute())

    print("Updated row is :", result)
    return result


# [END sqlalchemy_spanner_update_row]
