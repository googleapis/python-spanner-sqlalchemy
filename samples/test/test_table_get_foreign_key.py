# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from .. import table_get_foreign_key


def test_table_get_foreign_key(capsys, db_url, table_id_w_foreign_key):

    f_key = table_get_foreign_key.get_table_foreign_key(db_url, table_id_w_foreign_key)
    table_fk = list(table_id_w_foreign_key.foreign_keys)[0]
    out, err = capsys.readouterr()

    assert "Foreign key is:" in out
    assert f_key[0].get("name") == table_fk.name
