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


def new_field_value_question(question_name, field, ignore=False):

    name = question_name + ":new"

    ignore = lambda x: x[question_name] != "Enter new value"

    kwargs = dict(
        name=name, 
        message=field.description,
        default=field.default,
        ignore=ignore,
    )

    parser = lambda x: x

    if field.options is not None:
        question = inquirer.List(**kwargs, choices=field.options)

    if field.is_path:
        question = inquirer.Path(**kwargs, exists=True)

    elif field.type == bool:
        question = inquirer.Confirm(**kwargs)

    elif field.type == datetime:
        kwargs["message"] += f" ({parse.DATETIME_FORMAT})"
        validate = parse.validate_datetime_required if field.required else parse.validate_datetime
        question = inquirer.Text(**kwargs, validate=validate)
        parser = lambda x: parse.parse_datetime(None, current=x, required=field.required)

    elif field.type == int:
        validate = parse.validate_int_required if field.required else parse.validate_int
        question = inquirer.Text(**kwargs, validate=validate)
        parser = lambda x: parse.parse_int(None, current=x, required=field.required)

    elif field.type == float:
        validate = parse.validate_float_required if field.required else parse.validate_float
        question = inquirer.Text(**kwargs, validate=validate)
        parser = lambda x: parse.parse_float(None, current=x, required=field.required)
    
    else:
        question = inquirer.Text(**kwargs)

    return question, parser


def add_row(db, table_name):

    tbl = getattr(db, table_name)

    row = {}

    questions = []
    parsers = {}
    for field in tbl.fields:

        name = table_name + ":" + field.name
        choices = ["Enter new value", "Select from existing values"]

        if field.default is not None:
            choices.append(f"Select default: {field.default}")

        if not field.required:
            choices.append("Skip")

        question = inquirer.List(
            name=name, 
            message=field.description,
            choices=choices  
        )

        questions.append(question)

        # --- new value ---
        question, parser = new_field_value_question(name, field)
        questions.append(question)

        # --- new-value parser ---
        parsers[name] = parser

        # TODO: --- select from existing value
        #question = existing_value_question(name, field)
        #questions.append(question)

    answers = inquirer.prompt(questions)

    # parse 'new value' inputs
    if answers is None:
        parsed_answers = None

    else:
        parsed_answers = {}
        for name,value in answers.items():
            if name[-3:] == "new":
                name = name[:-4]   
                parsed_answers[name] = parsers[name](value)

            else:
                parsed_answers[name] = answers[name]


    print(parsed_answers)



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
