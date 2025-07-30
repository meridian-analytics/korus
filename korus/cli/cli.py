import inquirer
from korus.database import SQLiteDatabase
from korus.database.interface import TableViewer
import korus.cli.prompt as prompt
import korus.cli.parse as parse
import korus.cli.text as txt

# https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


def main(db):

    while True:
        try:
            table_name = prompt.select_table(db)

        except KeyboardInterrupt:
            break

        while True:
            try:
                tbl_action = prompt.table_action(table_name)
            except KeyboardInterrupt:
                break

            try:
                if tbl_action == prompt.TABLE_ADD:
                    add_row(db, table_name)

                    # TODO: make indentation marks green
                    print(txt.info(f"Successfully added row to {table_name} table."))

                elif tbl_action == prompt.TABLE_INFO:
                    tbl = getattr(db, table_name)
                    print(tbl)

                elif tbl_action == prompt.TABLE_CONTENTS:
                    view_contents(db, table_name)

            except KeyboardInterrupt:
                continue

    db.backend.close()


def view_contents(db, table_name):
    tbl = getattr(db, table_name)
    viewer = TableViewer(tbl)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1


def add_row(db, table_name) -> int:

    tbl = getattr(db, table_name)

    row = {}

    for field in tbl.fields:

        while True:
            action, kwargs = prompt.field_action(db, table_name, field)

            try:
                if action == prompt.FIELD_INFO:
                    print(field.info())

                if action == prompt.FIELD_ENTER:
                    value = prompt.enter_value(table_name, field, **kwargs)
                    break

                elif action == prompt.FIELD_SELECT:
                    value = prompt.select_value(table_name, field, **kwargs)
                    break

                elif action == prompt.FIELD_EXTERNAL:
                    view_contents(db, **kwargs)

                elif action == prompt.FIELD_SKIP:
                    value = None
                    break

            except KeyboardInterrupt:
                continue

        if value is not None:
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
