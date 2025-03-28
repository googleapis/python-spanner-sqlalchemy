# Copyright 2024 Google LLC All rights reserved.
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
from model import Singer, Album, Track

# Shows how INTERLEAVE IN PARENT can be used in SQLAlchemy.
# INTERLEAVE IN PARENT can be modelled as if it were a normal relationship
# in SQLAlchemy. SQLAlchemy can also generate the correct DDL for this.
#
# This sample uses the following table structure:
# CREATE TABLE albums (
# 	id STRING(36) NOT NULL,
# 	title STRING(200) NOT NULL,
# 	release_date DATE,
# 	singer_id STRING(36) NOT NULL,
# 	FOREIGN KEY(singer_id) REFERENCES singers (id)
# ) PRIMARY KEY (id);
#
# CREATE TABLE tracks (
# 	id STRING(36) NOT NULL,
# 	track_number INT64 NOT NULL,
# 	title STRING(200) NOT NULL,
# 	duration NUMERIC
# ) PRIMARY KEY (id, track_number),
# INTERLEAVE IN PARENT albums ON DELETE CASCADE
#
# See model.py for the full model definitions.
def interleaved_table():
    engine = create_engine(
        "spanner:///projects/sample-project/"
        "instances/sample-instance/"
        "databases/sample-database",
        echo=True,
    )
    with Session(engine) as session:
        # Insert a Singer row, two Albums, and six Tracks.
        # Track is INTERLEAVED IN PARENT Album.
        singer = Singer(
            id=str(uuid.uuid4()),
            first_name="John",
            last_name="Smith",
            albums=[
                Album(
                    id=str(uuid.uuid4()),
                    title="Rainforest",
                    tracks=[
                        # Track is INTERLEAVED IN PARENT Album, but can be treated
                        # as a normal relationship in SQLAlchemy.
                        Track(track_number=1, title="Green"),
                        Track(track_number=2, title="Blue"),
                        Track(track_number=3, title="Yellow"),
                    ],
                ),
                Album(
                    id=str(uuid.uuid4()),
                    title="Butterflies",
                    tracks=[
                        Track(track_number=1, title="Purple"),
                        Track(track_number=2, title="Cyan"),
                        Track(track_number=3, title="Mauve"),
                    ],
                ),
            ],
        )
        session.add(singer)
        session.commit()

        # Use AUTOCOMMIT for sessions that only read. This is more
        # efficient than using a read/write transaction to only read.
        session.connection(execution_options={"isolation_level": "AUTOCOMMIT"})
        # We can iterate over the tracks of an album as if it were a normal
        # relationship.
        print(f"Singer {singer.full_name} has these albums:")
        for album in singer.albums:
            print(f"\tAlbum {album.title} has these tracks:")
            for track in album.tracks:
                print(f"\t\t{track.track_number} - {track.title}")


if __name__ == "__main__":
    run_sample(interleaved_table)
