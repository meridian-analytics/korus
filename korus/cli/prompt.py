import sys
import inquirer
import readline
from datetime import datetime
import korus.cli.parse as parse


# tab completion for directory/file paths
# https://stackoverflow.com/a/56119373
readline.parse_and_bind("tab: complete")
readline.set_completer_delims("\t\n=")


def prompt_existing_value(question_name, field, tbl):
    name = question_name + ":value"

    values = tbl.get(fields=field.name)
    values = list(set(values))

    question = inquirer.List(name, message="Select value", choices=values)

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
        path = input(f"[?] {field.description}: ")  #TODO: make question mark yellow
        sys.stdout = original_stdout
        
        kwargs["message"] = "Validate path"
        kwargs["default"] = path
        question = inquirer.Path(**kwargs, exists=True)

    elif field.options is not None:
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
        validate = parse.validate_int_required if field.required else parse.validate_int
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == float:
        validate = (
            parse.validate_float_required if field.required else parse.validate_float
        )
        question = inquirer.Text(**kwargs, validate=validate)

    else:
        question = inquirer.Text(**kwargs)

    answers = inquirer.prompt([question])


    if answers is None:
        raise Exception

    return answers[name]
