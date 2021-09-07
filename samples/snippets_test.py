# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bs

from samples import snippets
from samples.conftest import insert_data
from sqlalchemy import (
    Column,
    create_engine,
    Integer,
    inspect,
    MetaData,
    String,
    Table,
)

DATA = [
    {"user_id": 1, "user_name": "abcdefg"},
    {"user_id": 2, "user_name": "ab/cdefg"},
    {"user_id": 3, "user_name": "ab%cdefg"},
    {"user_id": 4, "user_name": "ab_cdefg"},
    {"user_id": 5, "user_name": "abcde/fg"},
    {"user_id": 6, "user_name": "abcde%fg"},
]


def table_obj(database_url, tab_id):
    """Helper to produce a `Table` object for the given table id."""
    engine = create_engine(database_url)
    metadata = MetaData(bind=engine)

    table = Table(
        tab_id,
        metadata,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
    )
    return table


def test_enable_autocommit_mode(capsys, db_url):
    snippets.enable_autocommit_mode(db_url)

    out, err = capsys.readouterr()
    assert "Connection default mode is SERIALIZABLE" in out
    assert "Connection mode is now AUTOCOMMIT" in out


def test_create_table(capsys, db_url, table_id):
    snippets.create_table(db_url, table_id)

    out, err = capsys.readouterr()
    assert "Table {} successfully created".format(table_id) in out

    table = table_obj(db_url, table_id)
    assert table.exists() is True
    table.drop()


def test_drop_table(capsys, db_url, table_id):
    table = table_obj(db_url, table_id)
    table.create()

    snippets.drop_table(table)

    out, err = capsys.readouterr()
    assert "Table {} successfully dropped".format(table_id) in out
    assert table.exists() is False


def test_get_table_names(capsys, db_url, table):
    snippets.get_table_names(db_url)

    out, err = capsys.readouterr()
    assert "Retrieved table names:" in out
    assert table.name in out


def test_table_create_unique_index(capsys, db_url, table):
    snippets.create_unique_index(table)

    engine = create_engine(db_url)
    insp = inspect(engine)
    indexes = insp.get_indexes(table.name)

    out, err = capsys.readouterr()

    assert "Index created" in out
    assert indexes[0]["unique"] is True


def test_table_delete_all_rows(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.delete_all_rows(connection, table)

    out, err = capsys.readouterr()
    assert "Rows exist: 6" in out
    assert "Rows exist after deletion: 0" in out


def test_table_delete_row_with_where_condition(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.delete_row_with_where_condition(connection, table)

    out, err = capsys.readouterr()
    assert "Rows exist: 6" in out
    assert "Rows exist after deletion: 5" in out


def test_exists_table(capsys, table):
    snippets.table_exists(table)

    out, err = capsys.readouterr()
    assert "Table exists" in out


def test_table_fetch_rows(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.fetch_rows(connection, table)

    out, err = capsys.readouterr()
    assert "Fetched rows:" in out


def test_table_fetch_rows_with_limit_offset(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.fetch_rows_with_limit_offset(connection, table)

    out, err = capsys.readouterr()
    assert "Fetched row:" in out


def test_table_fetch_rows_with_order_by(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.fetch_rows_with_order_by(connection, table)

    out, err = capsys.readouterr()
    assert "Ordered rows:" in out


def test_table_fetch_row_with_where_condition(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.fetch_row_with_where_condition(connection, table)

    out, err = capsys.readouterr()
    assert "Fetched row:" in out
    assert len(rows) == 1


def test_table_filter_data_startswith(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.filter_data_startswith(connection, table)

    out, err = capsys.readouterr()
    assert "Fetched rows:" in out
    assert len(rows) == 3


def test_table_filter_data_with_contains(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.filter_data_with_contains(connection, table)

    out, err = capsys.readouterr()
    assert "Fetched rows:" in out
    assert len(rows) == 4


def test_table_filter_data_with_like(capsys, connection, table):
    insert_data(connection, table, DATA)
    rows = snippets.filter_data_with_like(connection, table)

    out, err = capsys.readouterr()
    assert "Fetched rows:" in out
    assert len(rows) == 3


def test_table_get_columns(capsys, db_url, table):
    snippets.get_table_columns(db_url, table)
    out, err = capsys.readouterr()
    assert "Fetched columns:" in out
    assert len(columns) == 2

    for single in columns:
        assert single["name"] in table.columns


def test_table_get_foreign_key(capsys, db_url, table_w_foreign_key):
    snippets.get_table_foreign_key(db_url, table_w_foreign_key)
    table_fk = list(table_w_foreign_key.foreign_keys)[0]
    out, err = capsys.readouterr()

    assert "Fetched foreign key:" in out
    assert f_key[0].get("name") == table_fk.name


def test_table_get_indexes(capsys, db_url, table):
    snippets.get_table_indexes(db_url, table)
    out, err = capsys.readouterr()
    table_index = list(table.indexes)[0].name

    assert "Fetched indexes:" in out
    assert indexes[0]["name"] == table_index


def test_table_get_primary_key(capsys, db_url, table):
    snippets.get_table_primary_key(db_url, table)
    out, err = capsys.readouterr()
    assert "Fetched primary key:" in out
    assert p_key.get("constrained_columns")[0] in table.primary_key.columns


def test_table_insert_row(capsys, connection, table):
    snippets.insert_row(connection, table)

    out, err = capsys.readouterr()
    assert "Inserted row:" in out
    assert len(rows) == 1


def test_table_update_row(capsys, connection, table):
    insert_data(connection, table, DATA)
    snippets.update_row(connection, table)

    out, err = capsys.readouterr()
    assert "Updated row:" in out
    assert "GEH" == rows[0][1]
