# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import argparse


def fetch_row_with_where_condition(table):
    """Fetch row with where condition from the table"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_id,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    table.insert().execute([
        {"user_id": 1, "user_name": 'ABC'},
        {"user_id": 2, "user_name": 'DEF'}
    ])

    # [START sqlalchemy_spanner_fetch_row_with_where_condition]
    result = list(table.select().where(table.c.user_id == 1).execute())

    print("Output is :", result)
    # [END sqlalchemy_spanner_fetch_row_with_where_condition]
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
    subparsers.add_parser("fetch_row_with_where_condition", help=fetch_row_with_where_condition.__doc__)
    args = parser.parse_args()
    if args.command == "fetch_row_with_where_condition":
        fetch_row_with_where_condition(args.table)
    else:
        print(f"Command {args.command} did not match expected commands.")
