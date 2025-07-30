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


def prompt_existing_value(question_name, field, values):
    name = question_name + ":value"
    question = inquirer.List(name, message=field.description, choices=values)
    answers = inquirer.prompt([question])

    if answers is None:
        raise Exception

    return answers[name]


def prompt_new_value(question_name, field):

    name = question_name + ":value"

    kwargs = dict(
        name=name,
        message=field.description,
        default=field.default,
    )

    if field.is_path:
        # reset stdout to allow tab-completion
        # https://stackoverflow.com/a/53260487
        original_stdout = sys.stdout
        sys.stdout = sys.__stdout__
        while True:
            path = input(
                f"[?] {field.description}: "
            )  # TODO: make question mark yellow
            if os.path.exists(path):
                break
            else:
                print(
                    ">> Invalid path, please try again."
                )  # TODO: make indentation marks red and text bold

        sys.stdout = original_stdout

        if path is None:
            answers = None

        else:
            answers = {name: path}

    else:
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
        raise Exception

    return answers[name]
