# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from .. import table_get_columns


def test_table_get_columns(capsys, db_url, table_id):

    columns = table_get_columns.get_table_columns(db_url, table_id)
    out, err = capsys.readouterr()
    assert "Columns are:" in out
    assert len(columns) is 2

    for single in columns:
        assert single['name'] in table_id.columns
