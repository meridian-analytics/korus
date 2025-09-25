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


def select_field_action(
    db: Database,
    table_name: str,
    field: FieldDefinition,
    msg: str = "Select field action",
):
    """Prompt user to select a field action.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
        field: korus.database.interface.FieldDefinition
            The field definition
        msg: str
            Prompt message

    Returns:
        : tuple(int, dict)
            The table action ENUM and any keyword args required for carrying out the action

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    tbl = getattr(db, table_name)

    # choices
    choices = {}

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
            msg = f"The {ext_name} table is empty. To add an entry to the {table_name} table, you must first add a {ext_name}."
            print(txt.error(msg))
            raise KeyboardInterrupt()

    else:
        choices["View info"] = (FIELD_INFO, {})
        choices["Enter value"] = (FIELD_ENTER, {})

        # if table has data, give user option to select from existing values
        existing_values = tbl.unique(field.name)
        if len(existing_values) > 0:
            kwargs = {"values": existing_values}
            choices["Select previously entered value"] = (FIELD_SELECT, kwargs)

    if not field.required:
        choices["Skip"] = (FIELD_SKIP, {})

    # form the question
    msg = str(cursor) + msg

    # prompt user
    choice = inquirer.list_input(message=msg, choices=list(choices.keys()))

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
    msg = str(cursor) + msg
    field_name = inquirer.list_input(message=msg, choices=tbl.field_names)
    field = tbl.fields_asdict[field_name]
    return field


def select_value(
    field: FieldDefinition, values: list, msg: str = "Select value"
) -> str:
    """Prompt user to select a value from a list of options.

    Args:
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
    msg = str(cursor) + msg
    value = inquirer.list_input(message=msg, choices=values)
    return parse.parse_value(value, field.type, field.required)


def enter_label(db: Database, taxonomy_id: int = None) -> list[tuple]:
    tax = db.taxonomy.get_taxonomy(taxonomy_id)

    VIEW_TAXONOMY = 0
    ENTER_LABEL = 1
    SELECT_ALL = 2

    labels = []
    while True:
        # sound sources
        choices = {
            "View taxonomy tree": VIEW_TAXONOMY,
            "Specify sound source": ENTER_LABEL,
        }
        msg = str(cursor) + "Specify which sounds were subject to exhaustive annotation"

        answer = inquirer.list_input(msg, choices=list(choices.keys()))
        choice = choices[answer]

        if choice == VIEW_TAXONOMY:
            tax.show()
            continue

        elif choice == ENTER_LABEL:
            msg = str(cursor) + "Enter sound-source label"

            def validate(answers, current):
                if tax.label_exists(current):
                    return True
                else:
                    reason = (
                        f"The taxonomy does not contain the sound source `{current}`"
                    )
                    raise inquirer.errors.ValidationError("", reason=reason)

            sound_source = inquirer.text(msg, validate=validate)

        # sound types
        while True:
            choices = {
                "View sound types": VIEW_TAXONOMY,
                "Specify sound type": ENTER_LABEL,
                "Select all sound types": SELECT_ALL,
            }
            msg = (
                str(cursor)
                + f"For the selected sound source ({sound_source}), specify which sound types were subject to exhaustive annotation"
            )

            answer = inquirer.list_input(msg, choices=list(choices.keys()))
            choice = choices[answer]

            if choice == VIEW_TAXONOMY:
                tax.sound_types(sound_source).show()
                continue

            elif choice == ENTER_LABEL:
                msg = str(cursor) + "Enter sound-type label"

                def validate(answers, current):
                    if tax.label_exists(sound_source, current):
                        return True
                    else:
                        reason = f"The taxonomy does not contain the sound type `{current}` for the sound source `{sound_source}`"
                        raise inquirer.errors.ValidationError("", reason=reason)

                sound_type = inquirer.text(msg, validate=validate)

            elif choice == SELECT_ALL:
                sound_type = "*"

            break

        labels.append((sound_source, sound_type))

        # ask user if they want to add another sound
        msg = str(cursor) + "Add another sound?"
        if not inquirer.confirm(msg):
            break

    return labels


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
    val_str = enter_value(field, validate=validate, msg=msg)
    return parse.parse_value(val_str, field.type, field.required)


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
    field: FieldDefinition,
    validate: callable = None,
    msg: str = "Enter value",
):
    """Prompt user to enter a field value.

    Args:
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
    msg = str(cursor) + msg

    # collect keyword args for inquirer methods
    kwargs = dict(
        message=msg,
        default=field.default,
    )

    if validate is None:
        validate = lambda answers, current: True

    validates = [validate]

    if field.is_path:
        return enter_path()

    if field.options is not None:
        value = inquirer.list_input(**kwargs, choices=field.options)

    elif field.type == bool:
        value = inquirer.confirm(**kwargs)

    elif field.type == datetime:
        kwargs["message"] += f" ({parse.DATETIME_FORMAT})"
        validates.insert(0, parse.create_validate_datetime(field.required))
        validate = parse.join(validates)
        value = inquirer.text(**kwargs, validate=validate)

    elif field.type == int:
        validates.insert(0, parse.create_validate_int(field.required))
        validate = parse.join(validates)
        value = inquirer.text(**kwargs, validate=validate)

    elif field.type == float:
        validates.insert(0, parse.create_validate_float(field.required))
        validate = parse.join(validates)
        value = inquirer.text(**kwargs, validate=validate)

    else:
        value = inquirer.text(**kwargs)

    if value is not None:
        value = parse.parse_value(value, field.type, field.required)

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
