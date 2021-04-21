# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import argparse


def table_exists(table):
    """Check the table exists"""
    # [START sqlalchemy_spanner_table_exists]

    # TODO(developer): Create the table
    # table = Table(
    #    table_id,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    result = table.exists()
    print("Table exists:", result)
    # [END sqlalchemy_spanner_table_exists]


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
    subparsers.add_parser("table_exists", help=table_exists.__doc__)
    args = parser.parse_args()
    if args.command == "table_exists":
        table_exists(args.table)
    else:
        print(f"Command {args.command} did not match expected commands.")
