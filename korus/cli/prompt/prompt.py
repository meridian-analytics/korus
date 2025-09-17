import os
import sys
import inquirer
import readline
import numpy as np
from datetime import datetime
import dateutil.parser
import datetime_glob
from korus.database.database import Database
from korus.database.interface import FieldDefinition
import korus.cli.parse as parse
import korus.cli.text as txt
from korus.cli.cursor import cursor


# tab completion for directory/file paths
# https://stackoverflow.com/a/56119373
readline.parse_and_bind("tab: complete")
readline.set_completer_delims("\t\n=,;")


# Field action ENUMs
FIELD_INFO = 0
FIELD_ENTER = 1
FIELD_SELECT = 2
FIELD_EXTERNAL = 3
FIELD_SKIP = 4

# Table action ENUMs
TABLE_INFO = 0
TABLE_CONTENTS = 1
TABLE_CONTENTS_DETAILED = 2
TABLE_ADD = 3
TABLE_UPDATE = 4


def select_table(db: Database) -> str:
    """Prompt user to select a table

    Args:
        db: korus.database.Database
            The database instance

    Returns:
        : str
            The selected table's name

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    message = str(cursor) + "Select table"
    choice = inquirer.list_input(message, choices=list(db.tables.keys()))
    if choice is None:
        raise KeyboardInterrupt

    return choice


def select_table_action(table_name: str) -> int:
    """Prompt user to select a table action.

    Args:
        table_name: str
            Table name

    Returns:
        : int
            Table action ENUM

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    # create list with choices
    choices = {
        "View info": TABLE_INFO,
        "View contents": TABLE_CONTENTS,
        "View contents (detailed)": TABLE_CONTENTS_DETAILED,
        "Add": TABLE_ADD,
        "Update": TABLE_UPDATE,
    }

    # prompt user to select from choices
    message = str(cursor) + "Select table action"
    choice = inquirer.list_input(message=message, choices=list(choices.keys()))
    if choice is None:
        raise KeyboardInterrupt

    return choices[choice]


def select_field_action(db: Database, table_name: str, field: FieldDefinition):
    """Prompt user to select a field action.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
        field: korus.database.interface.FieldDefinition
            The field definition

    Returns:
        : tuple(int, dict)
            The table action ENUM and any keyword args required for carrying out the action

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    tbl = getattr(db, table_name)

    # choices
    choices = {}
    choices["View info"] = (FIELD_INFO, {})

    if field.is_index:

        # get the name of the external/linked table
        ext_name = field.name[: field.name.rfind("_id")]

        # if the external table has data, give the user the option to view the data and/or enter a valid index
        ext_table = getattr(db, ext_name)
        if len(ext_table) > 0:
            kwargs = {"table_name": ext_name}
            choices[f"View {ext_name} table"] = (FIELD_EXTERNAL, kwargs)
            validate = parse.create_validate_index(ext_table)
            choices["Enter index"] = (FIELD_ENTER, {"validate": validate})

        # if the external table is empty, instruct the user to add some data to it
        else:
            msg = f"The {ext_name} table is empty. To add a row to the {table_name} table, you must first add a {ext_name}."
            print(txt.error(msg))
            raise KeyboardInterrupt()

    else:
        choices["Enter value"] = (FIELD_ENTER, {})

        # if table has data, give user option to select from existing values
        existing_values = tbl.unique(field.name)
        if len(existing_values) > 0:
            kwargs = {"values": existing_values}
            choices["Reuse existing value"] = (FIELD_SELECT, kwargs)

    if not field.required:
        choices["Skip"] = (FIELD_SKIP, {})

    # form the question
    message = str(cursor) + "Select field action"

    # prompt user
    choice = inquirer.list_input(message=message, choices=list(choices.keys()))

    if choice is None:
        raise KeyboardInterrupt

    return choices[choice]


def select_field(
    db: Database, table_name: str, msg: str = "Select field"
) -> FieldDefinition:
    """Prompt user to select a field from the table.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
        msg: str
            Prompt message

    Returns:
        field: korus.database.interface.FieldDefinition
            The field definition

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    tbl = getattr(db, table_name)
    name = table_name
    msg = str(cursor) + msg
    question = inquirer.List(name, message=msg, choices=tbl.field_names)
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    field_name = answers[name]
    field = tbl.fields_asdict[field_name]
    return field


def select_value(
    table_name: str, field: FieldDefinition, values: list, msg: str = "Select value"
) -> str:
    """Prompt user to select a value from a list of options.

    Args:
        table_name: str
            Table name
        field: korus.database.interface.FieldDefinition
            The field definition
        values: list
            The values to choose from
        msg: str
            Prompt message

    Returns:
        : any
            The selected and parsed value

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    name = table_name + ":" + field.name + ":value"
    question = inquirer.List(name, message=msg, choices=values)
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    value = answers[name]
    if value is not None:
        value = parse.parse_value(field, value)

    return value


def select_label(db, table_name, field_name):
    # TODO: implement this function
    pass


def enter_index(db: Database, table_name: str, msg: str = None) -> int:
    """Prompt user to enter a row index for the table.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
        msg: str
            Prompt message

    Returns:
        : int
            The validated and parsed index

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    tbl = getattr(db, table_name)
    field = FieldDefinition(
        name="id", description="Table index", type=int, required=True
    )
    validate = parse.create_validate_index(tbl)
    val_str = enter_value(table_name, field, validate=validate, msg=msg)
    return parse.parse_value(field, val_str)


def enter_path(multiple: bool = False, msg: str = "Enter path") -> str | list[str]:
    """Prompt user to enter a file or directory path.

    Checks that the path is valid.
    Allows for tab auto completion.
    Use comma as separator to enter multiple paths.
    Returns str if multiple=False, and a list of strings otherwise.

    Args:
        multiple: bool
            Allow multiple comma-separated input values
        msg: str
            Prompt message

    Returns:
        paths: str | list[str]
            The path(s).

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    # reset stdout to allow tab-completion
    # https://stackoverflow.com/a/53260487
    paths = []
    original_stdout = sys.stdout
    sys.stdout = sys.__stdout__
    while True:
        try:
            msg = str(cursor) + msg
            paths = input(txt.question(msg)).split(",")

        except KeyboardInterrupt:
            sys.stdout = original_stdout
            raise

        if not multiple and len(paths) > 1:
            print(txt.error("Multiple values not allowed, please try again."))
            continue

        exists = [os.path.exists(path) for path in paths]
        if np.all(exists):
            break

        else:
            print(txt.error("Invalid path, please try again."))

    sys.stdout = original_stdout

    if len(paths) == 1 and not multiple:
        paths = paths[0]

    return paths


def enter_value(
    table_name: str,
    field: FieldDefinition,
    validate: callable = None,
    msg: str = "Enter value",
):
    """Prompt user to enter a field value.

    Args:
        table_name: str
            Table name
        field: korus.database.interface.FieldDefinition
            The field definition
        validate: callable
            Validation function with signature fcn(answers, current) -> bool
        msg: str
            Prompt message

    Returns:
        value: any
            The validated and parsed value

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    name = table_name + ":" + field.name + ":value"
    msg = str(cursor) + msg

    # collect keyword args for inquirer methods
    kwargs = dict(
        name=name,
        message=msg,
        default=field.default,
    )

    if validate is None:
        validate = lambda answers, current: True

    validates = [validate]

    if field.is_path:
        return enter_path()

    if field.options is not None:
        question = inquirer.List(**kwargs, choices=field.options)

    elif field.type == bool:
        question = inquirer.Confirm(**kwargs)

    elif field.type == datetime:
        kwargs["message"] += f" ({parse.DATETIME_FORMAT})"
        validates.insert(0, parse.create_validate_datetime(field.required))
        validate = parse.join(validates)
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == int:
        validates.insert(0, parse.create_validate_int(field.required))
        validate = parse.join(validates)
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == float:
        validates.insert(0, parse.create_validate_float(field.required))
        validate = parse.join(validates)
        question = inquirer.Text(**kwargs, validate=validate)

    else:
        question = inquirer.Text(**kwargs)

    answers = inquirer.prompt([question])

    if answers is None:
        raise KeyboardInterrupt

    value = answers[name]
    if value is not None:
        value = parse.parse_value(field, value)

    return value


def select_timestamp_parser():
    """Prompt user to specify timestamp parsing method.

    https://labix.org/python-dateutil
    https://github.com/Parquery/datetime-glob

    OBS: datetime_glob cannot parse milliseconds

    Returns:
        fcn: callable
            Timestamp parser function with signature fcn(filename:str) -> datetime
    """

    AUTO = 0
    CUSTOM = 1
    NONE = 2

    choices = {
        "Automated": AUTO,
        "Custom": CUSTOM,
        "No timestamp": NONE,
    }

    questions = [
        inquirer.List(
            "method",
            message="Select method for parsing timestamp",
            choices=choices.keys(),
        ),
        inquirer.Text(
            "format",
            message="Enter custom timestamp format (glob pattern)",
            ignore=lambda x: choices[x["method"]] != CUSTOM,
        ),
    ]
    answers = inquirer.prompt(questions)

    if answers is None:
        raise KeyboardInterrupt

    choice = choices[answers["method"]]

    if choice is NONE:
        fcn = None

    elif choice is AUTO:

        def fcn(filename: str) -> datetime:
            try:
                return dateutil.parser.parse(filename, fuzzy=True)

            except:
                err_msg = f"Failed to parse timestamp from {filename}"
                raise ValueError(err_msg)

    else:
        matcher = datetime_glob.Matcher(pattern=answers["format"])

        def fcn(filename: str) -> datetime:
            dt = matcher.match(filename)
            if dt is None:
                err_msg = f"Failed to parse timestamp from {filename}"
                raise ValueError(err_msg)

            else:
                return dt

    return fcn
