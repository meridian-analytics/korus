import inquirer
from datetime import datetime
from korus.database import SQLiteDatabase

#https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


def create_question(table_name, field):

    name = table_name + "." + field.name

    kwargs = dict(
        name=name, 
        message=field.description,
        default=field.default,
    )

    if field.options is not None:
        q = inquirer.List(**kwargs, choices=field.options)

    if field.is_path:
        q = inquirer.Path(**kwargs)

    elif field.type == bool:
        q = inquirer.Confirm(**kwargs)

    elif field.type == datetime:
        q = inquirer.Text(**kwargs)

    else:
        q = inquirer.Text(**kwargs)

    return q    


def add_row(db, table_name):

    tbl = getattr(db, table_name)

    row = {}

    questions = []
    for field in tbl.fields:
        q = create_question(table_name, field)
        questions.append(q)

    answers = inquirer.prompt(questions)



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
