import inquirer
from korus.database import SQLiteDatabase
from korus.database.interface import TableViewer
import korus.cli.prompt as prompt
import korus.cli.parse as parse

# https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


def main(db):

    while True:
        try:
            tbl_name = prompt.select_table(db)

        except KeyboardInterrupt:
            break

        while True:
            try:
                tbl_action = prompt.table_action(tbl_name)
            except KeyboardInterrupt:
                break

            try:
                if tbl_action == prompt.TABLE_ADD:
                    add_row(db, tbl_name)

                elif tbl_action == prompt.TABLE_VIEW_INFO:
                    tbl = getattr(db, tbl_name)
                    print(tbl)

            except KeyboardInterrupt:
                continue

    db.backend.close()
    

def add_row(db, table_name) -> int:

    tbl = getattr(db, table_name)

    row = {}

    for field in tbl.fields:

        name = table_name + ":" + field.name

        while True:
            action, info = prompt.field_action(db, table_name, field)

            try:
                if action == prompt.FIELD_NEW:
                    value = prompt.prompt_new_value(name, field)

                elif action == prompt.FIELD_EXISTING:
                    value = prompt.prompt_existing_value(name, field, info["existing_values"])

                elif action == prompt.FIELD_VIEW:
                    ext_tbl = getattr(db, info["ext_name"])

                    viewer = TableViewer(ext_tbl)
                    for page in iter(viewer):
                        print(page)

                    continue

                elif action == prompt.FIELD_SKIP:
                    break

            except KeyboardInterrupt:
                continue

        row[field.name] = parse.parse_value(field, value)

    return tbl.add(row)


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
