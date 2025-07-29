import inquirer
from datetime import datetime
from korus.database import SQLiteDatabase
import korus.cli.parse as parse

#https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


'''
# tab completion for directory/file paths
# https://stackoverflow.com/a/56119373
import readline
readline.parse_and_bind("tab: complete")
readline.set_completer_delims("\t\n=")

# https://stackoverflow.com/a/53260487
import sys
original_stdout = sys.stdout
sys.stdout = sys.__stdout__
foo = input()
sys.stdout = original_stdout
'''


def prompt_existing_value(question_name, field, tbl):
    name = question_name + ":value"

    values = tbl.get(fields=field.name)
    values = list(set(values))

    question = inquirer.List(name, message="Select value", choices=values)

    answers = inquirer.prompt([question])

    if answers is None:
        raise Exception

    return answers[name]


def parse_value(field, value):
    if field.type == datetime:
        return parse.parse_datetime(None, current=value, required=field.required)

    elif field.type == int:
        return parse.parse_int(None, current=value, required=field.required)

    elif field.type == float:
        return parse.parse_float(None, current=value, required=field.required)
    
    else:
        return value



def prompt_new_value(question_name, field):

    name = question_name + ":value"

    kwargs = dict(
        name=name, 
        message="Enter value",
        default=field.default,
    )

    if field.options is not None:
        question = inquirer.List(**kwargs, choices=field.options)

    if field.is_path:
        # TODO: use python input() function to allow tab-completion
        question = inquirer.Path(**kwargs, exists=True)

    elif field.type == bool:
        question = inquirer.Confirm(**kwargs)

    elif field.type == datetime:
        kwargs["message"] += f" ({parse.DATETIME_FORMAT})"
        validate = parse.validate_datetime_required if field.required else parse.validate_datetime
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == int:
        validate = parse.validate_int_required if field.required else parse.validate_int
        question = inquirer.Text(**kwargs, validate=validate)

    elif field.type == float:
        validate = parse.validate_float_required if field.required else parse.validate_float
        question = inquirer.Text(**kwargs, validate=validate)
    
    else:
        question = inquirer.Text(**kwargs)

    answers = inquirer.prompt([question])

    if answers is None:
        raise Exception

    return answers[name]


def add_row(db, table_name):

    tbl = getattr(db, table_name)

    row = {}

    NEW = 0
    EXISTING = 1
    VIEW = 2
    SKIP = 3

    for field in tbl.fields:

        name = table_name + ":" + field.name
        choices = {"Enter a new value": NEW}

        if field.is_index:
            # both summary and detailed/iterator views?
            ext_tbl_name = field.name[:field.name.rfind("_id")]
            choices[f"View {ext_tbl_name}"] = VIEW
        
        else:
            #TODO: allow include this option if there are any existing values
            choices["Select from existing values"] = EXISTING

        if not field.required:
            choices["Skip"] = SKIP

        question = inquirer.List(
            name=name, 
            message=field.description,
            choices=list(choices.keys())  
        )

        while True:
            answers = inquirer.prompt([question])

            choice = choices[answers[name]]

            try:
                if choice == NEW:
                    value = prompt_new_value(name, field)

                elif choice == EXISTING:
                    value = prompt_existing_value(name, field, tbl)

                elif choice == SKIP:
                    break

            except:
                continue

            value = parse_value(field, value)

            row[field.name] = value
            break

    print(row)


def cli_fcn(db: SQLiteDatabase):

    def table_options(answers):
        if answers["table"] == "deployment":
            return ["a","b"]
        else:
            return ["c","d"]

    while True:

        questions = [
            inquirer.List(
                name="table", 
                message="Select a table",
                choices=["deployment", "annotation", "Exit"]    
            ),
            inquirer.List(
                name="options",
                message="Select an option",
                choices=table_options        
            )
        ]

        answers = inquirer.prompt(questions)

        if answers is None:
            print("Terminating ...")
            break

        print(answers)
