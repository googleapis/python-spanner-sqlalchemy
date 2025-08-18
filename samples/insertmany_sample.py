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


from datetime import datetime
import uuid
from sqlalchemy import text, String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


# To use SQLAlchemy 2.0's insertmany feature, models must have a
# unique column marked as an "insert_sentinal" with client-side
# generated values passed into it. This allows SQLAlchemy to perform a
# single bulk insert, even if the table has columns with server-side
# defaults which must be retrieved from a THEN RETURN clause, for
# operations like:
#
# with Session.begin() as session:
#     session.add(Singer(name="a"))
#     session.add(Singer(name="b"))
#
# Read more in the SQLAlchemy documentation of this feature:
# https://docs.sqlalchemy.org/en/20/core/connections.html#configuring-sentinel-columns


class Singer(Base):
    __tablename__ = "singers"
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        # Supply a unique UUID client-side
        default=lambda: str(uuid.uuid4()),
        # The column is unique and can be used as an insert_sentinel
        insert_sentinel=True,
        # Set a server-side default for write outside SQLAlchemy
        server_default=text("GENERATE_UUID()"),
    )
    name: Mapped[str]
    inserted_at: Mapped[datetime] = mapped_column(
        server_default=text("CURRENT_TIMESTAMP()")
    )
