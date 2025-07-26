import inquirer

#https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types


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
