# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import argparse


def fetch_rows_with_order_by(table):
    """fetch all rows from the table in order"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_id,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    table.insert().execute([
        {"user_id": 1, "user_name": 'GEH'},
        {"user_id": 2, "user_name": 'DEF'},
        {"user_id": 3, "user_name": 'HIJ'},
        {"user_id": 4, "user_name": 'ABC'}
    ])

    # [START sqlalchemy_spanner_fetch_rows_with_order_by]
    result = table.select().order_by(table.c.user_name).execute().fetchall()
    import pdb;pdb.set_trace()
    print("The order by result is:", result)
    # [END sqlalchemy_spanner_fetch_rows_with_order_by]
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
    subparsers.add_parser("fetch_rows_with_order_by", help=fetch_rows_with_order_by.__doc__)
    args = parser.parse_args()
    if args.command == "fetch_rows_with_order_by":
        fetch_rows_with_order_by(args.table)
    else:
        print(f"Command {args.command} did not match expected commands.")
