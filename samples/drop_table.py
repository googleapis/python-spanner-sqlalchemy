# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import argparse

from sqlalchemy import (
        Column,
        Integer,
        MetaData,
        String,
        Table,
        create_engine,
)


def drop_table(url, random_table_id):
    """Drop the table"""
    # [START sqlalchemy_spanner_drop_table]

    engine = create_engine(url)
    metadata = MetaData(bind=engine)

    table = Table(
        random_table_id,
        metadata,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
    )

    table.create()

    table.drop()
    print("Table {} is dropped successfully".format(table.name))
    # [END sqlalchemy_spanner_drop_table]
    return table


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("url", help="Your Cloud Spanner url which contains "
                                    "project-id, instance-id, databas-id.")
    parser.add_argument(
        "--table-id",
        help="Your Cloud Spanner table ID.",
        default="example_table",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("drop_table", help=drop_table.__doc__)
    args = parser.parse_args()
    if args.command == "drop_table":
        drop_table(args.url, args.table-id)
    else:
        print(f"Command {args.command} did not match expected commands.")

