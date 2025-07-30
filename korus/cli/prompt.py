import os
import sys
import inquirer
import readline
from datetime import datetime
import korus.cli.parse as parse


# tab completion for directory/file paths
# https://stackoverflow.com/a/56119373
readline.parse_and_bind("tab: complete")
readline.set_completer_delims("\t\n=")


FIELD_NEW = 0
FIELD_EXISTING = 1
FIELD_VIEW = 2
FIELD_SKIP = 3

TABLE_VIEW_INFO = 0
TABLE_VIEW_CONTENTS = 1
TABLE_ADD = 2


def select_table(db):
    name="main"
    question = inquirer.List(name, message="Select table", choices=list(db.tables.keys()))
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    return answers[name]


def table_action(table_name):
    name = table_name
    choices = {}
    choices["View info"] = TABLE_VIEW_INFO
    choices["View contents"] = TABLE_VIEW_CONTENTS
    choices["Add row"] = TABLE_ADD
    question = inquirer.List(name=table_name, message=f"[{table_name}] Select action", choices=list(choices.keys()))
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    return choices[answers[name]]


def field_action(db, table_name, field):
    name = table_name + ":" + field.name

    tbl = getattr(db, table_name)

    choices = {}

    if field.is_index:
        ext_name = field.name[: field.name.rfind("_id")]
        ext_tbl = getattr(db, ext_name)

        info = {"ext_name": ext_name}

        if len(ext_tbl) > 0:
            choices[f"View the {ext_name} table"] = (FIELD_VIEW, info)

    else:
        choices["Enter value"] = (FIELD_NEW, {})

        existing_values = tbl.unique(field.name)
        if len(existing_values) > 0:
            info = {"existing_values": existing_values}
            choices["Select value"] = (FIELD_EXISTING, info)

    if not field.required:
        choices["Skip"] = (FIELD_SKIP, {})

    question = inquirer.List(
        name=name, message=field.description, choices=list(choices.keys())
    )

    if len(choices) > 1:
        answers = inquirer.prompt([question])

        if answers is None:
            raise KeyboardInterrupt

        choice = choices[answers[name]]

    else:
        choice = choices[list(choices.keys())[0]]

    return choice


def prompt_existing_value(question_name, field, values):
    name = question_name + ":value"
    question = inquirer.List(name, message=field.description, choices=values)
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    return answers[name]


def prompt_path(question_name, field):
    # reset stdout to allow tab-completion
    # https://stackoverflow.com/a/53260487
    original_stdout = sys.stdout
    sys.stdout = sys.__stdout__
    while True:
        try:
            # TODO: make question mark yellow
            path = input(f"[?] {field.description}: ")  

        except KeyboardInterrupt:
            sys.stdout = original_stdout
            raise

        if os.path.exists(path):
            break

        else:
            # TODO: make indentation marks red and text bold
            print(">> Invalid path, please try again.")  

    sys.stdout = original_stdout
    return path


def prompt_new_value(question_name, field):

    name = question_name + ":value"

    kwargs = dict(
        name=name,
        message=field.description,
        default=field.default,
    )

    if field.is_path:
        return prompt_path(question_name, field)

    if field.options is not None:
        question = inquirer.List(**kwargs, choices=field.options)

    elif field.type == bool:
        question = inquirer.Confirm(**kwargs)

    elif field.type == datetime:
        kwargs["message"] += f" ({parse.DATETIME_FORMAT})"
        validate = (
            parse.validate_datetime_required
            if field.required
            else parse.validate_datetime
        )
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == int:
        validate = (
            parse.validate_int_required if field.required else parse.validate_int
        )
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == float:
        validate = (
            parse.validate_float_required
            if field.required
            else parse.validate_float
        )
        question = inquirer.Text(**kwargs, validate=validate)

    else:
        question = inquirer.Text(**kwargs)

    answers = inquirer.prompt([question])

    if answers is None:
        raise KeyboardInterrupt

    return answers[name]
