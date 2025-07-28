import inquirer
from datetime import datetime
from korus.database import SQLiteDatabase
import korus.cli.parse as parse

#https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


def create_question(table_name, field):

    name = table_name + "." + field.name

    kwargs = dict(
        name=name, 
        message=field.description,
        default=field.default,
    )

    parser = lambda x: x

    if field.options is not None:
        question = inquirer.List(**kwargs, choices=field.options)

    if field.is_path:
        question = inquirer.Path(**kwargs)

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
        question, parser = create_question(table_name, field)
        questions.append(question)
        parsers[question.name] = parser

    answers = inquirer.prompt(questions)

    # parse inputs
    if answers is not None:
        answers = {name: parsers[name](value) for name,value in answers.items()}

        print(answers)



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
