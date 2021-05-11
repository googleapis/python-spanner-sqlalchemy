# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from .. import table_get_primary_key


def test_table_get_primary_key(capsys, db_url, table_id):

    p_key = table_get_primary_key.get_table_primary_key(db_url, table_id)
    out, err = capsys.readouterr()
    assert "Primary key is:" in out
    assert p_key.get("constrained_columns")[0] in table_id.primary_key.columns
