# Copyright 2021 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import argparse


def update_row(table):
    """Update row in the table"""

    # TODO(developer): Create the table
    # table = Table(
    #    table_id,
    #    metadata,
    #    Column("user_id", Integer, primary_key=True),
    #    Column("user_name", String(16), nullable=False),
    # )
    # table.create()

    table.insert().execute(
        {"user_id": 1, "user_name": 'ABC'},
        {"user_id": 2, "user_name": 'DEF'},
    )

    # [START sqlalchemy_spanner_update_row]
    table.update().where(table.c.user_id == 2).values(
        user_name="GEH"
    ).execute()
    result = list(table.select().where(table.c.user_id == 2).execute())

    print("Updated row is :", result)
    # [END sqlalchemy_spanner_update_row]
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
    subparsers.add_parser("update_row", help=update_row.__doc__)
    args = parser.parse_args()
    if args.command == "update_row":
        update_row(args.table)
    else:
        print(f"Command {args.command} did not match expected commands.")