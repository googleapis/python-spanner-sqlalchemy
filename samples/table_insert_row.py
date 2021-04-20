# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import argparse


def insert_row(table):
    """Insert row in the table"""
    # [START sqlalchemy_spanner_insert_row]

    # TODO(developer): Create the table
    # table = Table(
    #    table_id,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    table.insert().execute({"user_id": 1, "user_name": 'David'})

    result = [row for row in table.select().execute()]

    print("Total rows:", result)
    # [END sqlalchemy_spanner_insert_row]
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--table",
        help="Your sqlalchemy table object.",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("insert_row", help=insert_row.__doc__)
    args = parser.parse_args()
    if args.command == "insert_row":
        insert_row(args.table)
    else:
        print(f"Command {args.command} did not match expected commands.")
