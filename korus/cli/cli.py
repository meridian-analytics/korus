import argparse
from tabulate import tabulate
from korus.database.database import Database, SQLiteDatabase
from .module import create_modules
from .cursor import cursor
import korus.cli.text as txt
from korus.__init__ import __version__


def welcome_message(path):
    msg = f"Welcome to the Korus command-line-interface v{__version__}."
    msg += f"\nConnected to: {path}"
    msg += "\nUse the prompts to view, add, or edit data."
    msg += "\nhttps://meridian-analytics.github.io/korus/"
    msg = tabulate([[msg]], tablefmt="double_grid")
    return msg


def exec_cli(db: Database):
    modules, id = create_modules(db)

    while id is not None:
        m = modules.get(id)
        cursor.go_to(m)
        id = cursor.execute()


def main():

    parser = argparse.ArgumentParser(
        prog="korus-cli",
        description="View, add, or modify data in a Korus SQLite database",
        epilog=None,
    )

    parser.add_argument("path", type=str, help="Path to the Korus SQLite database file")

    parser.add_argument(
        "-n",
        "--new",
        action="store_true",
        help="If the database file does not exist, create a new one",
    )

    args = parser.parse_args()

    try:
        db = SQLiteDatabase(args.path, new=args.new)
        print(welcome_message(args.path))
        exec_cli(db)
        db.backend.close()

    except OSError as err:
        err_msg = (
            f"{args.path} does not exist. Use `-n/--new` to create a new database."
        )
        print(txt.error(err_msg))


if __name__ == "__main__":
    main()
