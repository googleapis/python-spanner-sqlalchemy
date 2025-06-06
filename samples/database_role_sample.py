# Copyright 2025 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sample_helper import run_sample
from model import Singer


# Shows how to set the database role for a connection.
def database_role_sample():
    engine = create_engine(
        "spanner:///projects/sample-project/"
        "instances/sample-instance/"
        "databases/sample-database",
        # You can set the database role in the connect arguments.
        connect_args={"database_role": "my_role"},
        echo=True,
    )
    with Session(engine) as session:
        singer_id = str(uuid.uuid4())
        singer = Singer(id=singer_id, first_name="John", last_name="Doe")
        session.add(singer)
        session.commit()


if __name__ == "__main__":
    run_sample(database_role_sample)
