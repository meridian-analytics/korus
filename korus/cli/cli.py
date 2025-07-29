import inquirer
from korus.database import SQLiteDatabase
from korus.database.interface import TableViewer
import korus.cli.prompt as prompt
import korus.cli.parse as parse

# https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


def add_row(db, table_name):

    tbl = getattr(db, table_name)

    row = {}

    NEW = 0
    NEW_EXT = 1
    EXISTING = 2
    VIEW = 3
    SKIP = 4

    for field in tbl.fields:

        name = table_name + ":" + field.name
        choices = {}

        if field.is_index:
            ext_name = field.name[: field.name.rfind("_id")]
            ext_tbl = getattr(db, ext_name)

            if len(ext_tbl) > 0:
                choices[f"View the {ext_name} table"] = VIEW

            choices[f"Add a new entry to the {ext_name} table"] = NEW_EXT

        else:
            choices["Enter a new value"] = NEW

            if len(tbl) > 0:
                choices["Select from existing values"] = EXISTING

        if not field.required:
            choices["Skip"] = SKIP

        question = inquirer.List(
            name=name, message=field.description, choices=list(choices.keys())
        )

        while True:
            if len(choices) > 1:
                answers = inquirer.prompt([question])
                choice = choices[answers[name]]
            
            else:
                choice = choices[list(choices.keys())[0]]

            try:
                if choice == NEW:
                    value = prompt.prompt_new_value(name, field)

                elif choice == NEW_EXT:
                    # TODO: implement this
                    value = 1

                elif choice == EXISTING:
                    value = prompt.prompt_existing_value(name, field, tbl)

                elif choice == VIEW:
                    ext_tbl = getattr(db, ext_name)

                    viewer = TableViewer(ext_tbl)
                    for page in iter(viewer):
                        print(page)

                    continue

                elif choice == SKIP:
                    break

            except:
                continue

            value = parse.parse_value(field, value)

            row[field.name] = value
            break

    print(row)


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
                choices=["deployment", "annotation", "Exit"],
            ),
            inquirer.List(
                name="options", message="Select an option", choices=table_options
            ),
        ]

        answers = inquirer.prompt(questions)

        if answers is None:
            print("Terminating ...")
            break

        print(answers)
