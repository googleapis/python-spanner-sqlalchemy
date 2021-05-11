# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from .. import get_table_names


def test_get_table_name(capsys, db_url, table_id):

    tables = get_table_names.get_table_names(db_url)
    out, err = capsys.readouterr()
    assert "Table names are:" in out
    assert table_id.name in tables
