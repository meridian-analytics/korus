import inquirer
from korus.database import SQLiteDatabase
from korus.database.interface import TableViewer
import korus.cli.prompt as prompt
import korus.cli.parse as parse

# https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


def add_row(db, table_name) -> int:

    tbl = getattr(db, table_name)

    row = {}

    NEW = 0
    NEW_EXT = 1
    EXISTING = 2
    VIEW = 3
    SKIP = 4

    for field in tbl.fields:

        name = table_name + ":" + field.name

        action, info = prompt.prompt_field_action(db, table_name, field)

        if action == NEW:
            value = prompt.prompt_new_value(name, field)

        elif action == NEW_EXT:
            ext_tbl = getattr(db, info["ext_name"])
            idx = add_row(db, ext_tbl)
            if idx is None:
                return None

            value = str(idx)

        elif action == EXISTING:
            value = prompt.prompt_existing_value(name, field, info["existing_values"])

        elif action == VIEW:
            ext_tbl = getattr(db, info["ext_name"])

            viewer = TableViewer(ext_tbl)
            for page in iter(viewer):
                print(page)

            continue

        elif action == SKIP:
            break

    except:
        continue

    value = parse.parse_value(field, value)

    row[field.name] = value
    break

    # print(row)

    try:
        idx = tbl.add(row)

    except:
        print("Failed to add row to table")
        idx = None

    return idx


def cli_fcn(db: SQLiteDatabase):

    def table_options(answers):
        if answers["table"] == "deployment":
            return ["a", "b"]
        else:
            return ["c", "d"]

    while True:

        questions = [
            inquirer.List(
                name="table",
                message="Select a table",
                actions=["deployment", "annotation", "Exit"],
            ),
            inquirer.List(
                name="options", message="Select an option", actions=table_options
            ),
        ]

        answers = inquirer.prompt(questions)

        if answers is None:
            print("Terminating ...")
            break

        print(answers)
