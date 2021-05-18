# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from .. import table_get_indexes


def test_table_get_indexes(capsys, db_url, table_id):

    indexes = table_get_indexes.get_table_indexes(db_url, table_id)
    out, err = capsys.readouterr()
    table_index = list(table_id.indexes)[0].name

    assert "Indexes are:" in out
    assert indexes[0]['name'] == table_index
