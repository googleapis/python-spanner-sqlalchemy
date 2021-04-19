# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .. import autocommit


def test_enable_autocommit_mode(capsys, db_url):

    conn = autocommit.enable_autocommit_mode(db_url)
    out, err = capsys.readouterr()
    assert "Connection autocommit default mode is " in out
    assert "Spanner DBAPI default autocommit mode is False" in out

    assert conn.connection.connection.autocommit is True
    assert "Connection autocommit mode is " in out
    assert "Spanner DBAPI autocommit mode is True" in out
