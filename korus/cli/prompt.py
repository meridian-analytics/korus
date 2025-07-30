import os
import sys
import inquirer
import readline
from datetime import datetime
from korus.database.database import Database
from korus.database.interface import FieldDefinition
import korus.cli.parse as parse


# tab completion for directory/file paths
# https://stackoverflow.com/a/56119373
readline.parse_and_bind("tab: complete")
readline.set_completer_delims("\t\n=")


# Field action ENUMs
FIELD_INFO = 0
FIELD_ENTER = 1
FIELD_SELECT = 2
FIELD_EXTERNAL = 3
FIELD_SKIP = 4

# Table action ENUMs
TABLE_INFO = 0
TABLE_CONTENTS = 1
TABLE_ADD = 2


def header(table_name: str = None, field_name: str = None):
    h = ["main"]

    if table_name is not None:
        h.append(table_name)

    if field_name is not None:
        h.append(field_name)

    h = "|".join(h)
    if len(h) > 0:
        h = "[" + h + "] "

    return h


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
    name="main"
    question = inquirer.List(name, message="Select table", choices=list(db.tables.keys()))
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    return answers[name]


def table_action(table_name: str) -> int:
    """Prompt user to select a table action.
    
    Args:
        table_name: str
            Table name

    Returns:
        : int
            The table action ENUM

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    name = table_name
    choices = {}
    choices["View info"] = TABLE_INFO
    choices["View contents"] = TABLE_CONTENTS
    choices["Add row"] = TABLE_ADD
    message = header(table_name) + "Select table action"
    question = inquirer.List(name=table_name, message=message, choices=list(choices.keys()))
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    return choices[answers[name]]


def field_action(db: Database, table_name: str, field: FieldDefinition):
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

            def validate(answers, current):
                id = int(current)
                all_indices = ext_table.reset_filter().filter().indices
                if id not in all_indices:
                    raise inquirer.errors.ValidationError("", reason="Invalid index. Please enter a valid index.")
                
                return True

            choices["Enter index"] = (FIELD_ENTER, {"validate": validate})

        # if the external table is empty, instruct the user to add some data to it
        else:
            # TODO: make indentation marks red and text bold
            print(f">> The {ext_name} table is empty. To add a row to the {table_name} table, you must first add a {ext_name}.")
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
    name = table_name + ":" + field.name
    message = header(table_name, field.name) + "Select field action"
    question = inquirer.List(
        name=name, message=message, choices=list(choices.keys())
    )

    # prompt user
    answers = inquirer.prompt([question])

    if answers is None:
        raise KeyboardInterrupt

    return choices[answers[name]]


def select_value(table_name: str, field: FieldDefinition, values: list) -> str:
    """ Prompt user to select a value from a list of options.
    
    Args:
        table_name: str
            Table name
        field: korus.database.interface.FieldDefinition
            The field definition
        values: list
            The values to choose from

    Returns:
        : str
            The string representation of the selected value

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C            
    """
    name = table_name + ":" + field.name + ":value"
    message = header(table_name, field.name) + "Select value"
    question = inquirer.List(name, message=message, choices=values)
    answers = inquirer.prompt([question])
    if answers is None:
        raise KeyboardInterrupt

    return answers[name]


def enter_path(table_name, field):
    """ Prompt user to enter a file or directory path.

    Checks that the path is valid.
    Allows for tab auto completion.
    
    Args:
        table_name: str
            Table name
        field: korus.database.interface.FieldDefinition
            The field definition

    Returns:
        path: str
            The path

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C            
    """
    # reset stdout to allow tab-completion
    # https://stackoverflow.com/a/53260487
    original_stdout = sys.stdout
    sys.stdout = sys.__stdout__
    while True:
        try:
            # TODO: make question mark yellow
            message = header(table_name, field.name) + "Enter path"
            path = input(f"[?] {message}: ") 

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


def enter_value(table_name, field, validate=None):
    """ Prompt user to enter a field value.
    
    Args:
        table_name: str
            Table name
        field: korus.database.interface.FieldDefinition
            The field definition

    Returns:
        : str
            The input value

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C            
    """

    name = table_name + ":" + field.name + ":value"
    message = header(table_name, field.name) + "Enter value"

    kwargs = dict(
        name=name,
        message=message,
        default=field.default,
    )

    if validate is None:
        validate = lambda answers, current: True

    validates = [validate]

    if field.is_path:
        return enter_path(table_name, field)

    if field.options is not None:
        question = inquirer.List(**kwargs, choices=field.options)

    elif field.type == bool:
        question = inquirer.Confirm(**kwargs)

    elif field.type == datetime:
        kwargs["message"] += f" ({parse.DATETIME_FORMAT})"
        validates.append(
            parse.validate_datetime_required
            if field.required
            else parse.validate_datetime
        )
        
        validate = parse.validation_chain(validates)
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == int:
        validates.append(
            parse.validate_int_required if field.required else parse.validate_int
        )
        validate = parse.validation_chain(validates)
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == float:
        validates.append(
            parse.validate_float_required
            if field.required
            else parse.validate_float
        )
        validate = parse.validation_chain(validates)
        question = inquirer.Text(**kwargs, validate=validate)

    else:
        question = inquirer.Text(**kwargs)

    answers = inquirer.prompt([question])

    if answers is None:
        raise KeyboardInterrupt

    return answers[name]
