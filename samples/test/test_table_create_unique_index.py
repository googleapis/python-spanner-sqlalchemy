# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from .. import table_create_unique_index

from sqlalchemy import create_engine, inspect


def test_table_create_unique_index(capsys, db_url, table_id):
    table_create_unique_index.create_unique_indexes(table_id)

    engine = create_engine(db_url)
    insp = inspect(engine)
    indexes = insp.get_indexes(table_id.name)

    out, err = capsys.readouterr()

    assert "Index created successfully" in out
    assert indexes[0]['unique'] is True
